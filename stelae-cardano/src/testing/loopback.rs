//! A disposable OCI registry and Mithril listing on a loopback port.
//!
//! It speaks what the transport speaks over real sockets, keeps what it is sent
//! under one directory and counts it, so a test or a measurement can hold a
//! repository to another byte for byte without Docker. A rule can fail, drop or
//! hold any request, which is how a real process gets killed at a chosen point
//! of a publication. It keeps blobs per repository, as a registry does, so two
//! repositories under one server don't see each other's uploads.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, RwLock,
    },
};

use serde::{Deserialize, Serialize};
use stelae::digest::{digest_reader, Digest};

const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

/// What a repository was sent while this server ran.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accounting {
    /// Each digest's size, and how many uploads completed under it.
    pub uploads: BTreeMap<String, (u64, u64)>,
    /// Blob bytes that crossed the wire: completed, refused and interrupted
    /// uploads alike, so what exceeds [`Self::uploaded`] went for nothing.
    pub received: u64,
    /// Manifest tags in the order they were written, with what each then held.
    pub tags: Vec<(String, String)>,
}

impl Accounting {
    /// Bytes of the uploads that completed.
    pub fn uploaded(&self) -> u64 {
        self.uploads
            .values()
            .map(|(size, count)| size * count)
            .sum()
    }
}

/// A request as a rule sees it.
#[derive(Debug)]
pub struct Seen<'a> {
    pub method: &'a str,
    pub path: &'a str,
}

/// What to do about a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Serve it.
    Proceed,
    /// Answer this status and do nothing.
    Refuse(u16),
    /// Do nothing until [`Loopback::release`], then serve it: a write that
    /// lands after whoever sent it has gone.
    Hold,
    /// Do nothing until [`Loopback::release`], then drop the connection with
    /// the request unserved: a write that never lands.
    HoldUnserved,
    /// Serve it, then hold the answer until [`Loopback::release`], then drop
    /// the connection: a write whose answer was lost.
    HoldAnswer,
}

type Rule = Arc<dyn Fn(&Seen<'_>) -> Verdict + Send + Sync>;

struct Shared {
    root: PathBuf,
    port: u16,
    highest_immutable: AtomicU64,
    uploads: AtomicU64,
    /// Names each manifest write's file before it moves under its tag.
    partials: AtomicU64,
    /// Past this size a blob isn't written or checked, only counted. Zero keeps
    /// everything.
    discard_above: AtomicU64,
    accounting: Mutex<BTreeMap<String, Accounting>>,
    rule: RwLock<Option<Rule>>,
    held: Mutex<HoldState>,
    wake: Condvar,
}

#[derive(Default)]
struct HoldState {
    waiting: u64,
    released: bool,
}

pub struct Loopback {
    shared: Arc<Shared>,
}

impl Loopback {
    /// Serve `root` on a free port until the process ends.
    pub fn serve(root: &Path) -> io::Result<Self> {
        Self::serve_on(root, 0)
    }

    pub fn serve_on(root: &Path, port: u16) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let shared = Arc::new(Shared {
            root: root.to_owned(),
            port: listener.local_addr()?.port(),
            highest_immutable: AtomicU64::new(0),
            uploads: AtomicU64::new(0),
            partials: AtomicU64::new(0),
            discard_above: AtomicU64::new(0),
            accounting: Mutex::default(),
            rule: RwLock::new(None),
            held: Mutex::default(),
            wake: Condvar::new(),
        });
        let accepting = shared.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let shared = accepting.clone();
                std::thread::spawn(move || {
                    // A peer killed mid-request is what some tests are for.
                    let _ = connection(stream, &shared);
                });
            }
        });
        Ok(Self { shared })
    }

    pub fn port(&self) -> u16 {
        self.shared.port
    }

    /// The URL of a repository on this server.
    pub fn repository(&self, name: &str) -> String {
        format!("oci://127.0.0.1:{}/{name}", self.port())
    }

    /// The URL a Mithril client lists this server's one snapshot from.
    pub fn aggregator(&self) -> String {
        format!("http://127.0.0.1:{}/aggregator", self.port())
    }

    /// What the Mithril listing offers. Left at 0 it offers nothing past files
    /// already on disk, so a run reads only those and fetches nothing.
    pub fn offer_immutable(&self, highest: u64) {
        self.shared
            .highest_immutable
            .store(highest, Ordering::SeqCst);
    }

    /// Stop writing blobs past `bytes`, keeping only their size, so the
    /// registry costs a measurement nothing on the volume it measures. Such
    /// a blob can't be read back: a restore of what this server took fails.
    pub fn discard_above(&self, bytes: u64) {
        self.shared.discard_above.store(bytes, Ordering::SeqCst);
    }

    pub fn accounting(&self, name: &str) -> Accounting {
        let accounting = self.shared.accounting.lock().unwrap();
        accounting.get(name).cloned().unwrap_or_default()
    }

    /// The manifest bytes under a tag.
    pub fn manifest(&self, name: &str, tag: &str) -> Option<Vec<u8>> {
        fs::read(tag_path(&self.shared.root, name, tag)).ok()
    }

    /// Every tag a repository holds, with its manifest's bytes.
    pub fn manifests(&self, name: &str) -> BTreeMap<String, Vec<u8>> {
        let tags = self.shared.root.join(name).join("manifests/tags");
        let Ok(entries) = fs::read_dir(tags) else {
            return BTreeMap::new();
        };
        entries
            .flatten()
            .filter_map(|entry| {
                let tag = entry.file_name().to_str()?.to_owned();
                Some((tag, fs::read(entry.path()).ok()?))
            })
            .collect()
    }

    /// Every blob a repository holds, as digest and size.
    pub fn blobs(&self, name: &str) -> BTreeMap<String, u64> {
        let blobs = self.shared.root.join(name).join("blobs/sha256");
        let Ok(entries) = fs::read_dir(blobs) else {
            return BTreeMap::new();
        };
        entries
            .flatten()
            .filter_map(|entry| {
                let hex = entry.file_name().to_str()?.to_owned();
                Some((format!("sha256:{hex}"), entry.metadata().ok()?.len()))
            })
            .collect()
    }

    /// Every digest a tag's manifest names that the repository doesn't hold at
    /// the size the manifest gives: what a restore of that tag would miss.
    pub fn absent_blobs(&self, name: &str) -> BTreeMap<String, u64> {
        let held = self.blobs(name);
        let mut absent = BTreeMap::new();
        for manifest in self.manifests(name).values() {
            let manifest: serde_json::Value = serde_json::from_slice(manifest).unwrap_or_default();
            let layers = manifest["layers"].as_array().cloned().unwrap_or_default();
            for named in std::iter::once(&manifest["config"]).chain(&layers) {
                let digest = named["digest"].as_str().unwrap_or_default().to_owned();
                let size = named["size"].as_u64().unwrap_or_default();
                if held.get(&digest) != Some(&size) {
                    absent.insert(digest, size);
                }
            }
        }
        absent
    }

    /// Apply a rule to every request from now on. A rule is told nothing about
    /// what came before, so one that counts holds its own count.
    pub fn rule(&self, rule: impl Fn(&Seen<'_>) -> Verdict + Send + Sync + 'static) {
        let mut held = self.shared.held.lock().unwrap();
        held.released = false;
        held.waiting = 0;
        *self.shared.rule.write().unwrap() = Some(Arc::new(rule));
    }

    /// Serve everything from now on, including what a rule holds.
    pub fn release(&self) {
        *self.shared.rule.write().unwrap() = None;
        self.shared.held.lock().unwrap().released = true;
        self.shared.wake.notify_all();
    }

    /// Wait until a rule holds a request, or `patience` runs out.
    pub fn wait_for_hold(&self, patience: std::time::Duration) -> bool {
        let held = self.shared.held.lock().unwrap();
        let (held, _) = self
            .shared
            .wake
            .wait_timeout_while(held, patience, |held| held.waiting == 0)
            .unwrap();
        held.waiting > 0
    }
}

fn tag_path(root: &Path, name: &str, tag: &str) -> PathBuf {
    root.join(name).join("manifests/tags").join(tag)
}

/// Whether a part of a request's path stays under the root once joined to it:
/// relative, with no empty, `.` or `..` component.
fn confined(part: &str) -> bool {
    !part.contains('\\')
        && part
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."))
}

/// A digest's hex, if it is nothing else.
fn digest_hex(digest: &str) -> Option<&str> {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    (!hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(hex)
}

fn held_until_released(shared: &Shared) {
    let mut held = shared.held.lock().unwrap();
    held.waiting += 1;
    shared.wake.notify_all();
    while !held.released {
        held = shared.wake.wait(held).unwrap();
    }
}

struct Head {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
}

impl Head {
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or_default()
    }

    fn query(&self, name: &str) -> Option<&str> {
        self.target
            .split_once('?')?
            .1
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    }
}

fn read_head(reader: &mut impl BufRead) -> io::Result<Option<Head>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    let (method, target) = (method.to_owned(), target.to_owned());
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    Ok(Some(Head {
        method,
        target,
        headers,
    }))
}

/// Copy a request's body into `sink`, returning how many bytes it held.
fn copy_body(reader: &mut impl BufRead, head: &Head, sink: &mut dyn Write) -> io::Result<u64> {
    if head
        .headers
        .get("transfer-encoding")
        .is_some_and(|encoding| encoding.eq_ignore_ascii_case("chunked"))
    {
        let mut total = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let size = line.trim().split(';').next().unwrap_or_default();
            let size = u64::from_str_radix(size, 16)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            if size == 0 {
                let mut end = String::new();
                reader.read_line(&mut end)?;
                return Ok(total);
            }
            io::copy(&mut reader.by_ref().take(size), sink)?;
            let mut end = String::new();
            reader.read_line(&mut end)?;
            total += size;
        }
    }
    let length = head
        .headers
        .get("content-length")
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let copied = io::copy(&mut reader.by_ref().take(length), sink)?;
    if copied < length {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(copied)
}

/// A writer that remembers how much went through it.
struct Counted<'a> {
    sink: &'a mut dyn Write,
    bytes: u64,
}

impl Write for Counted<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.sink.write(buffer)?;
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

/// A writer into `file` that keeps the first `keep` bytes and only counts the
/// rest, so a blob past the limit costs the volume nothing.
struct Capped<'a> {
    file: &'a mut File,
    keep: u64,
    seen: u64,
}

impl Write for Capped<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let room = self.keep.saturating_sub(self.seen);
        let kept = usize::try_from(room).map_or(buffer.len(), |room| room.min(buffer.len()));
        self.file.write_all(&buffer[..kept])?;
        self.seen += buffer.len() as u64;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Copy a blob's body into `sink`, charging its repository for what arrived
/// whether or not all of it did.
fn receive_blob(
    reader: &mut impl BufRead,
    head: &Head,
    shared: &Shared,
    name: &str,
    sink: &mut dyn Write,
) -> io::Result<u64> {
    let mut counted = Counted { sink, bytes: 0 };
    let copied = copy_body(reader, head, &mut counted);
    let mut accounting = shared.accounting.lock().unwrap();
    accounting.entry(name.to_owned()).or_default().received += counted.bytes;
    copied
}

/// The repository a request sends blob bytes to, if it does.
fn upload_target(head: &Head) -> Option<&str> {
    let rest = head.path().strip_prefix("/v2/")?;
    let (name, _) = rest.rsplit_once("/blobs/uploads/")?;
    matches!(head.method.as_str(), "PATCH" | "PUT").then_some(name)
}

struct Answer {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Payload,
}

enum Payload {
    Bytes(Vec<u8>),
    File(File, u64),
}

impl Answer {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Payload::Bytes(Vec::new()),
        }
    }

    fn with(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type", "application/json".to_owned())],
            body: Payload::Bytes(body),
        }
    }

    fn absent(code: &str) -> Self {
        let body =
            format!(r#"{{"errors":[{{"code":"{code}","message":"unknown","detail":{{}}}}]}}"#);
        Self::json(404, body.into_bytes())
    }
}

fn write_answer(stream: &mut TcpStream, answer: Answer, head_only: bool) -> io::Result<()> {
    let length = match &answer.body {
        Payload::Bytes(bytes) => bytes.len() as u64,
        Payload::File(_, length) => *length,
    };
    let mut text = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {length}\r\n",
        answer.status,
        match answer.status {
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            _ => "Status",
        }
    );
    for (name, value) in &answer.headers {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    text.push_str("\r\n");
    stream.write_all(text.as_bytes())?;
    if head_only {
        return Ok(());
    }
    match answer.body {
        Payload::Bytes(bytes) => stream.write_all(&bytes),
        Payload::File(mut file, _) => io::copy(&mut file, stream).map(|_| ()),
    }
}

fn connection(stream: TcpStream, shared: &Shared) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(1024 * 1024, stream.try_clone()?);
    let mut writer = stream;
    while let Some(head) = read_head(&mut reader)? {
        let verdict = shared
            .rule
            .read()
            .unwrap()
            .as_ref()
            .map_or(Verdict::Proceed, |rule| {
                rule(&Seen {
                    method: &head.method,
                    path: head.path(),
                })
            });
        if matches!(verdict, Verdict::Hold | Verdict::HoldUnserved) {
            held_until_released(shared);
        }
        if verdict == Verdict::HoldUnserved {
            return Ok(());
        }
        let answer = match verdict {
            Verdict::Refuse(status) => {
                match upload_target(&head) {
                    Some(name) => receive_blob(&mut reader, &head, shared, name, &mut io::sink())?,
                    None => copy_body(&mut reader, &head, &mut io::sink())?,
                };
                Answer::status(status)
            }
            _ => route(&head, &mut reader, shared)?,
        };
        if verdict == Verdict::HoldAnswer {
            held_until_released(shared);
            return Ok(());
        }
        write_answer(&mut writer, answer, head.method == "HEAD")?;
    }
    Ok(())
}

fn route(head: &Head, reader: &mut impl BufRead, shared: &Shared) -> io::Result<Answer> {
    let path = head.path();
    if path == "/v2/" {
        return Ok(Answer::json(200, b"{}".to_vec()));
    }
    if path == "/aggregator/artifact/cardano-database" {
        return Ok(Answer::json(200, listing(shared)));
    }
    if path == "/accounting" {
        let accounting = shared.accounting.lock().unwrap();
        return Ok(Answer::json(200, serde_json::to_vec(&*accounting)?));
    }
    let Some(rest) = path.strip_prefix("/v2/") else {
        copy_body(reader, head, &mut io::sink())?;
        return Ok(Answer::status(404));
    };
    // A request names files under the root, and only there.
    let leaf = |part: &str| !part.contains('/') && (part.is_empty() || confined(part));
    if let Some((name, id)) = rest.rsplit_once("/blobs/uploads/") {
        if !confined(name) || !leaf(id) {
            return refuse_outside(head, reader);
        }
        return upload(head, reader, shared, name, id);
    }
    if let Some((name, digest)) = rest.rsplit_once("/blobs/") {
        if !confined(name) || digest_hex(digest).is_none() {
            return refuse_outside(head, reader);
        }
        return blob(head, shared, name, digest);
    }
    if let Some((name, reference)) = rest.rsplit_once("/manifests/") {
        let named = match reference.strip_prefix("sha256:") {
            Some(_) => digest_hex(reference).is_some(),
            None => !reference.is_empty() && leaf(reference),
        };
        if !confined(name) || !named {
            return refuse_outside(head, reader);
        }
        return manifest(head, reader, shared, name, reference);
    }
    copy_body(reader, head, &mut io::sink())?;
    Ok(Answer::status(404))
}

fn refuse_outside(head: &Head, reader: &mut impl BufRead) -> io::Result<Answer> {
    copy_body(reader, head, &mut io::sink())?;
    let body = br#"{"errors":[{"code":"NAME_INVALID","message":"outside the root","detail":{}}]}"#;
    Ok(Answer::json(400, body.to_vec()))
}

/// One snapshot's listing: the client reads only its beacon, and needs the rest
/// to parse.
fn listing(shared: &Shared) -> Vec<u8> {
    let highest = shared.highest_immutable.load(Ordering::SeqCst);
    serde_json::to_vec(&serde_json::json!([{
        "hash": "loopback",
        "merkle_root": "loopback",
        "beacon": { "epoch": 0, "immutable_file_number": highest },
        "certificate_hash": "loopback",
        "total_db_size_uncompressed": 0,
        "cardano_node_version": "0.0.0",
        "created_at": "2026-01-01T00:00:00Z"
    }]))
    .expect("a literal serializes")
}

fn upload(
    head: &Head,
    reader: &mut impl BufRead,
    shared: &Shared,
    name: &str,
    id: &str,
) -> io::Result<Answer> {
    let dir = shared.root.join(name).join("uploads");
    let location = |id: &str| {
        format!(
            "http://127.0.0.1:{}/v2/{name}/blobs/uploads/{id}",
            shared.port
        )
    };
    match head.method.as_str() {
        "POST" => {
            copy_body(reader, head, &mut io::sink())?;
            fs::create_dir_all(&dir)?;
            let id = shared.uploads.fetch_add(1, Ordering::SeqCst).to_string();
            File::create(dir.join(&id))?;
            Ok(Answer::status(202)
                .with("Location", location(&id))
                .with("Docker-Upload-UUID", id)
                .with("Range", "0-0"))
        }
        "PATCH" | "PUT" => {
            let path = dir.join(id);
            let mut file = File::options().append(true).open(&path)?;
            let discard_above = shared.discard_above.load(Ordering::SeqCst);
            let length: Option<u64> = head
                .headers
                .get("content-length")
                .and_then(|length| length.parse().ok());
            // A body of unknown length is kept until it passes the limit.
            let keep = match (discard_above, length) {
                (0, _) => u64::MAX,
                (limit, Some(length)) if length > limit => 0,
                (limit, _) => limit,
            };
            let start = file.metadata()?.len();
            let mut capped = Capped {
                file: &mut file,
                keep,
                seen: 0,
            };
            let received = receive_blob(reader, head, shared, name, &mut capped)?;
            if received > keep {
                file.set_len(start + received)?;
            }
            drop(file);
            if head.method == "PATCH" {
                let size = fs::metadata(&path)?.len();
                return Ok(Answer::status(202)
                    .with("Location", location(id))
                    .with("Docker-Upload-UUID", id)
                    .with("Range", format!("0-{}", size.saturating_sub(1))));
            }
            finish_upload(shared, name, &path, head.query("digest"))
        }
        _ => {
            copy_body(reader, head, &mut io::sink())?;
            Ok(Answer::status(405))
        }
    }
}

fn finish_upload(
    shared: &Shared,
    name: &str,
    path: &Path,
    digest: Option<&str>,
) -> io::Result<Answer> {
    let digest = digest.map(|digest| digest.replace("%3A", ":"));
    let size = fs::metadata(path)?.len();
    let discard_above = shared.discard_above.load(Ordering::SeqCst);
    let trusted = discard_above > 0 && size > discard_above;
    let found = match digest.clone() {
        // Taken on trust, but still only as a name under the root.
        Some(digest) if trusted && digest_hex(&digest).is_some() => digest,
        _ => {
            let (found, _) = digest_reader(File::open(path)?).map_err(io::Error::other)?;
            let found = format!("sha256:{}", found.to_hex());
            if digest.as_deref() != Some(found.as_str()) {
                fs::remove_file(path)?;
                let body =
                    br#"{"errors":[{"code":"DIGEST_INVALID","message":"no match","detail":{}}]}"#;
                return Ok(Answer::json(400, body.to_vec()));
            }
            found
        }
    };
    let blobs = shared.root.join(name).join("blobs/sha256");
    fs::create_dir_all(&blobs)?;
    let hex = found.trim_start_matches("sha256:");
    fs::rename(path, blobs.join(hex))?;
    let mut accounting = shared.accounting.lock().unwrap();
    let repository = accounting.entry(name.to_owned()).or_default();
    repository
        .uploads
        .entry(found.clone())
        .or_insert((size, 0))
        .1 += 1;
    Ok(Answer::status(201)
        .with(
            "Location",
            format!("http://127.0.0.1:{}/v2/{name}/blobs/{found}", shared.port),
        )
        .with("Docker-Content-Digest", found))
}

fn blob(head: &Head, shared: &Shared, name: &str, digest: &str) -> io::Result<Answer> {
    let hex = digest.trim_start_matches("sha256:");
    let path = shared.root.join(name).join("blobs/sha256").join(hex);
    let Ok(file) = File::open(&path) else {
        return Ok(Answer::absent("BLOB_UNKNOWN"));
    };
    let length = file.metadata()?.len();
    let answer = Answer {
        status: 200,
        headers: vec![
            ("Content-Type", "application/octet-stream".to_owned()),
            ("Docker-Content-Digest", digest.to_owned()),
        ],
        body: Payload::File(file, length),
    };
    Ok(match head.method.as_str() {
        "GET" | "HEAD" => answer,
        _ => Answer::status(405),
    })
}

fn manifest(
    head: &Head,
    reader: &mut impl BufRead,
    shared: &Shared,
    name: &str,
    reference: &str,
) -> io::Result<Answer> {
    let root = shared.root.join(name).join("manifests");
    let digests = root.join("digests");
    let path = match reference.strip_prefix("sha256:") {
        Some(hex) => digests.join(hex),
        None => root.join("tags").join(reference),
    };
    if head.method == "PUT" {
        let mut body = Vec::new();
        copy_body(reader, head, &mut body)?;
        let digest = Digest::compute(&body).to_hex();
        fs::create_dir_all(root.join("tags"))?;
        fs::create_dir_all(&digests)?;
        fs::write(digests.join(&digest), &body)?;
        // Moved into place, so a read of the tag sees one manifest or the other,
        // from a file no other write shares.
        let write = shared.partials.fetch_add(1, Ordering::SeqCst);
        let partial = root.join(format!(".partial-{write}"));
        fs::write(&partial, &body)?;
        fs::rename(&partial, &path)?;
        let mut accounting = shared.accounting.lock().unwrap();
        let repository = accounting.entry(name.to_owned()).or_default();
        let digest = format!("sha256:{digest}");
        repository.tags.push((reference.to_owned(), digest.clone()));
        return Ok(Answer::status(201)
            .with(
                "Location",
                format!(
                    "http://127.0.0.1:{}/v2/{name}/manifests/{digest}",
                    shared.port
                ),
            )
            .with("Docker-Content-Digest", digest));
    }
    copy_body(reader, head, &mut io::sink())?;
    let Ok(body) = fs::read(&path) else {
        return Ok(Answer::absent("MANIFEST_UNKNOWN"));
    };
    let digest = format!("sha256:{}", Digest::compute(&body).to_hex());
    Ok(Answer {
        status: 200,
        headers: vec![
            ("Content-Type", MANIFEST_MEDIA_TYPE.to_owned()),
            ("Docker-Content-Digest", digest),
        ],
        body: Payload::Bytes(body),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "cardano/counted";

    /// Send one upload's head and `body`, promising `length` bytes, then hang
    /// up without reading an answer.
    fn send(loopback: &Loopback, id: &str, length: usize, body: &[u8]) {
        let mut stream = TcpStream::connect(("127.0.0.1", loopback.port())).unwrap();
        let path = format!("/v2/{NAME}/blobs/uploads/{id}?digest=sha256:0");
        let head = format!("PUT {path} HTTP/1.1\r\nContent-Length: {length}\r\n\r\n");
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
    }

    fn start_upload(loopback: &Loopback) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", loopback.port())).unwrap();
        let head = format!("POST /v2/{NAME}/blobs/uploads/ HTTP/1.1\r\nContent-Length: 0\r\n\r\n");
        stream.write_all(head.as_bytes()).unwrap();
        let mut answer = String::new();
        let mut reader = BufReader::new(stream);
        while reader.read_line(&mut answer).unwrap() > 2 {}
        let line = answer
            .lines()
            .find_map(|line| line.strip_prefix("Docker-Upload-UUID: "));
        line.expect("an upload id").to_owned()
    }

    fn eventually_received(loopback: &Loopback, expected: u64) {
        let began = std::time::Instant::now();
        while loopback.accounting(NAME).received != expected {
            assert!(
                began.elapsed() < std::time::Duration::from_secs(30),
                "received {}, not {expected}",
                loopback.accounting(NAME).received
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    #[test]
    fn a_refused_upload_and_an_interrupted_one_still_count_as_received() {
        let root = tempfile::tempdir().unwrap();
        let loopback = Loopback::serve(root.path()).unwrap();

        // Cut off: 40 of the 100 bytes it promised.
        let id = start_upload(&loopback);
        send(&loopback, &id, 100, &[7; 40]);
        eventually_received(&loopback, 40);

        // Refused: all of it arrived, and none of it was kept.
        loopback.rule(|request| match request.method {
            "PUT" => Verdict::Refuse(503),
            _ => Verdict::Proceed,
        });
        send(&loopback, "refused", 25, &[7; 25]);
        eventually_received(&loopback, 65);

        let accounting = loopback.accounting(NAME);
        assert_eq!(accounting.uploaded(), 0);
        assert!(accounting.uploads.is_empty());
    }

    /// Send one request and read the status it was answered with.
    fn status_of(loopback: &Loopback, head: &str, body: &[u8]) -> u16 {
        let mut stream = TcpStream::connect(("127.0.0.1", loopback.port())).unwrap();
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        line.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    fn put_blob(loopback: &Loopback, framing: &str, body: &[u8]) -> (String, u16) {
        let id = start_upload(loopback);
        let digest = format!("sha256:{}", Digest::compute(body).to_hex());
        let path = format!("/v2/{NAME}/blobs/uploads/{id}?digest={digest}");
        let head = format!("PUT {path} HTTP/1.1\r\n{framing}\r\n\r\n");
        (digest, status_of(loopback, &head, body))
    }

    #[test]
    fn a_request_naming_a_path_outside_the_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("registry");
        let loopback = Loopback::serve(&root).unwrap();
        fs::write(dir.path().join("secret"), b"not the registry's").unwrap();
        let id = start_upload(&loopback);
        for (method, target) in [
            ("PUT", format!("/v2/{NAME}/manifests/../../../escaped")),
            ("PUT", "/v2/../escaped/manifests/latest".to_owned()),
            ("PUT", format!("/v2/{NAME}/manifests/sha256:../../escaped")),
            (
                "PUT",
                format!("/v2/{NAME}/blobs/uploads/../../{id}?digest=sha256:00"),
            ),
            (
                "GET",
                format!("/v2/{NAME}/blobs/sha256:../../../../../secret"),
            ),
        ] {
            let head = format!("{method} {target} HTTP/1.1\r\nContent-Length: 2\r\n\r\n");
            assert_eq!(status_of(&loopback, &head, b"{}"), 400, "{method} {target}");
        }
        let mut beside: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name())
            .collect();
        beside.sort();
        assert_eq!(beside, ["registry", "secret"]);
    }

    #[test]
    fn a_body_of_unknown_length_is_kept_until_it_passes_the_discard_limit() {
        let root = tempfile::tempdir().unwrap();
        let loopback = Loopback::serve(root.path()).unwrap();
        loopback.discard_above(10);
        for (body, kept) in [(&b"small"[..], true), (&[7u8; 25][..], false)] {
            let mut framed = format!("{:x}\r\n", body.len()).into_bytes();
            framed.extend_from_slice(body);
            framed.extend_from_slice(b"\r\n0\r\n\r\n");
            let id = start_upload(&loopback);
            let digest = format!("sha256:{}", Digest::compute(body).to_hex());
            let path = format!("/v2/{NAME}/blobs/uploads/{id}?digest={digest}");
            let head = format!("PUT {path} HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n");
            assert_eq!(status_of(&loopback, &head, &framed), 201);
            let stored = fs::read(
                root.path()
                    .join(NAME)
                    .join("blobs/sha256")
                    .join(&digest[7..]),
            );
            let stored = stored.unwrap();
            assert_eq!(stored.len(), body.len());
            assert_eq!(stored == body, kept, "{} bytes", body.len());
        }
        assert_eq!(loopback.accounting(NAME).received, 30);
    }

    #[test]
    fn a_manifest_naming_a_blob_the_repository_lacks_is_reported() {
        let root = tempfile::tempdir().unwrap();
        let loopback = Loopback::serve(root.path()).unwrap();
        let (held, status) = put_blob(&loopback, "Content-Length: 4", b"held");
        assert_eq!(status, 201);
        let missing = format!("sha256:{}", "0".repeat(64));
        let manifest = serde_json::json!({
            "config": { "digest": held, "size": 4 },
            "layers": [{ "digest": held, "size": 4 }, { "digest": missing, "size": 3 }],
        })
        .to_string();
        let length = manifest.len();
        let head =
            format!("PUT /v2/{NAME}/manifests/latest HTTP/1.1\r\nContent-Length: {length}\r\n\r\n");
        assert_eq!(status_of(&loopback, &head, manifest.as_bytes()), 201);
        assert_eq!(loopback.absent_blobs(NAME), BTreeMap::from([(missing, 3)]));
    }
}
