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
    /// Blob bytes received, in completed uploads or not.
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
    /// Do nothing until [`Loopback::release`], then serve it.
    Hold,
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
        if verdict == Verdict::Hold {
            held_until_released(shared);
        }
        let answer = match verdict {
            Verdict::Refuse(status) => {
                copy_body(&mut reader, &head, &mut io::sink())?;
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
    if let Some((name, id)) = rest.rsplit_once("/blobs/uploads/") {
        return upload(head, reader, shared, name, id);
    }
    if let Some((name, digest)) = rest.rsplit_once("/blobs/") {
        return blob(head, shared, name, digest);
    }
    if let Some((name, reference)) = rest.rsplit_once("/manifests/") {
        return manifest(head, reader, shared, name, reference);
    }
    copy_body(reader, head, &mut io::sink())?;
    Ok(Answer::status(404))
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
            let received = if discard_above > 0 && length.unwrap_or(u64::MAX) > discard_above {
                let received = copy_body(reader, head, &mut io::sink())?;
                file.set_len(file.metadata()?.len() + received)?;
                received
            } else {
                copy_body(reader, head, &mut file)?
            };
            drop(file);
            {
                let mut accounting = shared.accounting.lock().unwrap();
                accounting.entry(name.to_owned()).or_default().received += received;
            }
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
    let found = match digest.clone() {
        Some(digest) if discard_above > 0 && size > discard_above => digest,
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
        // Moved into place, so a read of the tag sees one manifest or the other.
        let partial = root.join(format!(".partial-{}", std::process::id()));
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
