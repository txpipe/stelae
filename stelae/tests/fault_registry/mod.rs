//! An in-process OCI registry that can break the transfers it serves.
//!
//! Enough of the distribution API for a [`stelae::oci::Registry`] to publish a
//! snapshot and pull it back over plaintext HTTP — monolithic blob uploads,
//! blob `HEAD`/`GET`, manifest `PUT`/`GET` — plus scripted faults on the reads:
//! a body cut off partway, a body that is honestly short, or a status code, and
//! a wait before a layer blob's or a manifest's first byte. A write can be
//! refused with a status, and a stored manifest forgotten. No container, so the
//! read-retry, read-ahead and interrupted-seal tests run under plain `cargo
//! test`.
//!
//! Repositories are not kept apart: one instance serves one repository.
//!
//! Every response closes its connection, so each attempt the client makes is a
//! fresh request that this server sees and counts.

use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest as _, Sha256};

/// One way to spoil a `GET`.
#[derive(Debug, Clone, Copy)]
pub enum Fault {
    /// Announce the whole length, send half of it, and close.
    Truncate,
    /// Announce half the length and send exactly that.
    Short,
    /// Answer with this status and no content.
    Status(u16),
}

#[derive(Default)]
struct Script {
    once: VecDeque<Fault>,
    always: Option<Fault>,
}

impl Script {
    fn next(&mut self) -> Option<Fault> {
        self.once.pop_front().or(self.always)
    }
}

#[derive(Default)]
struct State {
    blobs: HashMap<String, Vec<u8>>,
    manifests: HashMap<String, (String, Vec<u8>)>,
    /// Layer digests of the snapshot last published, in manifest order: its
    /// diff manifest's, then its tip manifest's.
    diff_layers: Vec<String>,
    tip_layers: Vec<String>,
    /// Every layer digest a stored manifest has named, in the order first
    /// named: a chain's layers, oldest sequence first.
    listed: Vec<String>,
    config: Option<String>,
    /// Statuses that answer every `PUT` of a key (a blob digest, or
    /// `manifests/<reference>`).
    refused: HashMap<String, u16>,
    /// Keyed by blob digest, or `manifests/<reference>`.
    faults: HashMap<String, Script>,
    /// Every blob `HEAD` answered with a fault.
    head_fault: Option<u16>,
    /// `GET`s per key, and `HEAD`s per blob digest.
    gets: HashMap<String, usize>,
    heads: HashMap<String, usize>,
    uploads: usize,
    /// Blob digests whose upload completed, in that order.
    pushed: Vec<String>,
    /// The wait before a layer blob's first byte, by digest, or before a
    /// manifest's, by `manifests/<reference>`.
    delays: HashMap<String, Duration>,
    /// Layer blob `GET`s in the order they arrived, and in the order their
    /// answers were done.
    arrived: Vec<String>,
    served: Vec<String>,
    /// Layer blob `GET`s being answered now, and the most there were at once.
    active: usize,
    peak: usize,
    /// Manifest `GET`s being answered now, and the most there were at once.
    manifests_active: usize,
    manifests_peak: usize,
    /// Every tag listing answered whole, whatever its `n` and `last` say.
    unpaged: bool,
}

pub struct FaultRegistry {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl FaultRegistry {
    pub fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));

        let serving = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let state = Arc::clone(&serving);
                std::thread::spawn(move || {
                    let _ = handle(stream, &state);
                });
            }
        });

        Self { address, state }
    }

    pub fn url(&self, repository: &str) -> String {
        format!("oci://{}/{repository}", self.address)
    }

    /// Where this registry listens, as a registry client names it.
    pub fn address(&self) -> String {
        self.address.to_string()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The layer blob digests of the snapshot last published: those its diff
    /// manifest lists, then those its tip manifest lists.
    pub fn layers(&self) -> Vec<String> {
        let state = self.state();
        state
            .diff_layers
            .iter()
            .chain(&state.tip_layers)
            .cloned()
            .collect()
    }

    /// Every layer blob digest a manifest stored here has named, in the order
    /// first named. For a chain published in order, the layers of its oldest
    /// sequence first.
    pub fn listed(&self) -> Vec<String> {
        self.state().listed.clone()
    }

    /// The manifest stored under `reference` (a tag or a digest), as stored.
    pub fn manifest(&self, reference: &str) -> Option<Vec<u8>> {
        self.state()
            .manifests
            .get(reference)
            .map(|(_, body)| body.clone())
    }

    /// Every tag a manifest is stored under, sorted.
    pub fn tags(&self) -> Vec<String> {
        let mut tags: Vec<String> = self
            .state()
            .manifests
            .keys()
            .filter(|reference| !reference.starts_with("sha256:"))
            .cloned()
            .collect();
        tags.sort();
        tags
    }

    /// The blob stored under `digest`.
    pub fn blob(&self, digest: &str) -> Option<Vec<u8>> {
        self.state().blobs.get(digest).cloned()
    }

    /// Store `body` under the tag `reference`, as a writer that skipped every
    /// check would.
    pub fn put_manifest(&self, reference: &str, body: Vec<u8>) {
        let media_type = "application/vnd.oci.image.manifest.v1+json".to_owned();
        self.state()
            .manifests
            .insert(reference.to_owned(), (media_type, body));
    }

    /// Answer every tag listing with every tag, ignoring `n` and `last`.
    pub fn list_tags_unpaged(&self) {
        self.state().unpaged = true;
    }

    /// Drop the manifest stored under the tag `reference`, as a registry
    /// deleting it would.
    pub fn forget_manifest(&self, reference: &str) {
        self.state().manifests.remove(reference);
    }

    /// Answer every `PUT` of `key` (a blob digest, or `manifests/<reference>`)
    /// with `status`, storing nothing, until [`FaultRegistry::accept_puts`].
    pub fn refuse_puts(&self, key: &str, status: u16) {
        self.state().refused.insert(key.to_owned(), status);
    }

    /// Accept every `PUT` again.
    pub fn accept_puts(&self) {
        self.state().refused.clear();
    }

    /// Blob digests uploaded so far, layers and configs alike, in the order
    /// their uploads completed.
    pub fn pushed(&self) -> Vec<String> {
        self.state().pushed.clone()
    }

    /// The config blob digest of the manifest last pushed.
    pub fn config(&self) -> String {
        self.state().config.clone().expect("nothing was published")
    }

    /// Spoil the next `GET`s of `key` (a digest or `manifests/<reference>`).
    pub fn fail_next(&self, key: &str, faults: &[Fault]) {
        let mut state = self.state();
        let script = state.faults.entry(key.to_owned()).or_default();
        script.once.extend(faults.iter().copied());
    }

    /// Spoil every `GET` of `key`.
    pub fn fail_always(&self, key: &str, fault: Fault) {
        self.state()
            .faults
            .entry(key.to_owned())
            .or_default()
            .always = Some(fault);
    }

    /// Answer every blob `HEAD` with `status`.
    pub fn fail_heads(&self, status: u16) {
        self.state().head_fault = Some(status);
    }

    /// How many `GET`s `key` received.
    pub fn gets(&self, key: &str) -> usize {
        self.state().gets.get(key).copied().unwrap_or(0)
    }

    /// How many `HEAD`s each blob digest received.
    pub fn heads(&self) -> HashMap<String, usize> {
        self.state().heads.clone()
    }

    /// Wait `delay` before the first byte of every `GET` of these keys: blob
    /// digests, or `manifests/<reference>`.
    pub fn delay(&self, digests: &[String], delay: Duration) {
        let mut state = self.state();
        for digest in digests {
            state.delays.insert(digest.clone(), delay);
        }
    }

    /// Layer blob `GET`s in the order they arrived.
    pub fn arrived(&self) -> Vec<String> {
        self.state().arrived.clone()
    }

    /// Layer blob `GET`s in the order their answers were done.
    pub fn served(&self) -> Vec<String> {
        self.state().served.clone()
    }

    /// The most layer blob `GET`s answered at once.
    pub fn peak(&self) -> usize {
        self.state().peak
    }

    /// The most manifest `GET`s answered at once.
    pub fn manifests_peak(&self) -> usize {
        self.state().manifests_peak
    }

    /// Forget the layer `GET`s logged so far, and the peak.
    pub fn forget(&self) {
        let mut state = self.state();
        state.arrived.clear();
        state.served.clear();
        state.peak = state.active;
        state.manifests_peak = state.manifests_active;
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &TcpStream) -> std::io::Result<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));

    let mut headers = HashMap::new();
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let mut body = Vec::new();
    if let Some(length) = headers.get("content-length") {
        body.resize(length.parse().unwrap_or(0), 0);
        reader.read_exact(&mut body)?;
    } else if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size)?;
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk)?;
            if size == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..size]);
        }
    }

    Ok(Request {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        headers,
        body,
    })
}

fn respond(
    mut stream: &TcpStream,
    status: u16,
    headers: &[(&str, String)],
    length: usize,
    body: &[u8],
) -> std::io::Result<()> {
    let mut head =
        format!("HTTP/1.1 {status} X\r\nConnection: close\r\nContent-Length: {length}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");

    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Both)
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn decode(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| v.replace("%3A", ":").replace("%3a", ":"))
    })
}

fn handle(stream: TcpStream, state: &Mutex<State>) -> std::io::Result<()> {
    let request = read_request(&stream)?;
    let lock = || state.lock().unwrap_or_else(|p| p.into_inner());
    let path = request.path.as_str();

    if path == "/v2/" || path == "/v2" {
        return respond(&stream, 200, &[], 2, b"{}");
    }

    let Some(rest) = path.strip_prefix("/v2/") else {
        return respond(&stream, 404, &[], 0, b"");
    };

    if rest.ends_with("/tags/list") {
        let (unpaged, tags) = {
            let state = lock();
            let mut tags: Vec<String> = state
                .manifests
                .keys()
                .filter(|reference| !reference.starts_with("sha256:"))
                .cloned()
                .collect();
            tags.sort();
            (state.unpaged, tags)
        };
        let page: Vec<String> = match unpaged {
            true => tags,
            false => {
                let last = decode(&request.query, "last").unwrap_or_default();
                let n = decode(&request.query, "n").and_then(|n| n.parse().ok());
                tags.into_iter()
                    .filter(|tag| *tag > last)
                    .take(n.unwrap_or(usize::MAX))
                    .collect()
            }
        };
        let body = serde_json::to_vec(&serde_json::json!({ "name": "", "tags": page })).unwrap();
        return respond(&stream, 200, &[], body.len(), &body);
    }

    if let Some(index) = rest.find("/blobs/uploads/") {
        let repository = &rest[..index];
        return match request.method.as_str() {
            "POST" => {
                let id = {
                    let mut state = lock();
                    state.uploads += 1;
                    state.uploads
                };
                let location = format!("/v2/{repository}/blobs/uploads/{id}");
                respond(&stream, 202, &[("Location", location)], 0, b"")
            }
            "PUT" => {
                let digest = decode(&request.query, "digest").unwrap_or_default();
                if let Some(status) = lock().refused.get(&digest).copied() {
                    return respond(&stream, status, &[], 0, b"");
                }
                {
                    let mut state = lock();
                    state.blobs.insert(digest.clone(), request.body);
                    state.pushed.push(digest.clone());
                }
                let location = format!("/v2/{repository}/blobs/{digest}");
                respond(&stream, 201, &[("Location", location)], 0, b"")
            }
            _ => respond(&stream, 405, &[], 0, b""),
        };
    }

    if let Some(index) = rest.find("/blobs/") {
        let digest = rest[index + "/blobs/".len()..].to_owned();
        let (blob, fault) = {
            let mut state = lock();
            let blob = state.blobs.get(&digest).cloned();
            match request.method.as_str() {
                "HEAD" => {
                    *state.heads.entry(digest.clone()).or_default() += 1;
                    (blob, state.head_fault.map(Fault::Status))
                }
                _ => {
                    *state.gets.entry(digest.clone()).or_default() += 1;
                    let fault = state.faults.get_mut(&digest).and_then(Script::next);
                    (blob, fault)
                }
            }
        };

        let head = request.method == "HEAD";

        // Only layer `GET`s are logged and delayed, so a test reads the order a
        // restore fetched its layers in without the config blob in it.
        let layer = !head && lock().listed.contains(&digest);
        if layer {
            let delay = {
                let mut state = lock();
                state.arrived.push(digest.clone());
                state.active += 1;
                state.peak = state.peak.max(state.active);
                state.delays.get(&digest).copied()
            };

            if let Some(delay) = delay {
                std::thread::sleep(delay);
            }
        }

        let answered = match (blob, fault) {
            (_, Some(Fault::Status(code))) => respond(&stream, code, &[], 0, b""),
            (None, _) => respond(&stream, 404, &[], 0, b""),
            (Some(blob), _) if head => {
                let mut stream = &stream;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                    blob.len()
                );
                stream.write_all(head.as_bytes())?;
                stream.shutdown(std::net::Shutdown::Both)
            }
            (Some(blob), fault) => serve(&stream, &[], &blob, fault),
        };

        if layer {
            let mut state = lock();
            state.active -= 1;
            state.served.push(digest);
        }

        return answered;
    }

    if let Some(index) = rest.find("/manifests/") {
        let reference = rest[index + "/manifests/".len()..].to_owned();
        let key = format!("manifests/{reference}");

        if request.method == "PUT" {
            if let Some(status) = lock().refused.get(&key).copied() {
                return respond(&stream, status, &[], 0, b"");
            }

            let media_type = request
                .headers
                .get("content-type")
                .cloned()
                .unwrap_or_default();
            let digest = sha256(&request.body);
            let parsed: serde_json::Value = serde_json::from_slice(&request.body).unwrap();

            let layers: Vec<String> = parsed["layers"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|layer| layer["digest"].as_str().unwrap().to_owned())
                .collect();
            let config = parsed["config"]["digest"].as_str().map(str::to_owned);

            // A manifest of another inscription starts another snapshot. A tip
            // manifest is pushed first, so its layers wait for the diff's.
            let mut state = lock();
            if state.config != config {
                state.diff_layers.clear();
                state.tip_layers.clear();
            }
            for layer in &layers {
                if !state.listed.contains(layer) {
                    state.listed.push(layer.clone());
                }
            }
            if reference.ends_with(stelae::TIP_SUFFIX) {
                state.tip_layers = layers;
            } else {
                state.diff_layers = layers;
            }
            state.config = config;
            let entry = (media_type, request.body);
            state.manifests.insert(digest.clone(), entry.clone());
            state.manifests.insert(reference, entry);
            drop(state);

            let location = format!("/v2/{}/manifests/{digest}", &rest[..index]);
            return respond(
                &stream,
                201,
                &[("Location", location), ("Docker-Content-Digest", digest)],
                0,
                b"",
            );
        }

        let (manifest, fault, delay) = {
            let mut state = lock();
            *state.gets.entry(key.clone()).or_default() += 1;
            state.manifests_active += 1;
            state.manifests_peak = state.manifests_peak.max(state.manifests_active);
            let fault = state.faults.get_mut(&key).and_then(Script::next);
            let delay = state.delays.get(&key).copied();
            (state.manifests.get(&reference).cloned(), fault, delay)
        };

        if let Some(delay) = delay {
            std::thread::sleep(delay);
        }

        let answered = match (manifest, fault) {
            (_, Some(Fault::Status(code))) => respond(&stream, code, &[], 0, b""),
            (None, _) => {
                let body = br#"{"errors":[{"code":"MANIFEST_UNKNOWN","message":"unknown"}]}"#;
                respond(&stream, 404, &[], body.len(), body)
            }
            (Some((media_type, body)), fault) => {
                let headers = [
                    ("Content-Type", media_type),
                    ("Docker-Content-Digest", sha256(&body)),
                ];
                serve(&stream, &headers, &body, fault)
            }
        };

        lock().manifests_active -= 1;

        return answered;
    }

    respond(&stream, 404, &[], 0, b"")
}

/// A `200` carrying `body`, spoiled as `fault` says.
fn serve(
    stream: &TcpStream,
    headers: &[(&str, String)],
    body: &[u8],
    fault: Option<Fault>,
) -> std::io::Result<()> {
    let half = &body[..body.len() / 2];

    match fault {
        None => respond(stream, 200, headers, body.len(), body),
        Some(Fault::Truncate) => respond(stream, 200, headers, body.len(), half),
        Some(Fault::Short) => respond(stream, 200, headers, half.len(), half),
        Some(Fault::Status(_)) => unreachable!("answered before the body"),
    }
}
