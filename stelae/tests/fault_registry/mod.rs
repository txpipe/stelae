//! An in-process OCI registry that can break the transfers it serves.
//!
//! Enough of the distribution API for a [`stelae::oci::Registry`] to publish a
//! stele and pull it back over plaintext HTTP — monolithic blob uploads, blob
//! `HEAD`/`GET`, manifest `PUT`/`GET` — plus scripted faults on the reads: a
//! body cut off partway, a body that is honestly short, or a status code. No
//! container, so the read-retry tests run under plain `cargo test`.
//!
//! Every response closes its connection, so each attempt the client makes is a
//! fresh request that this server sees and counts.

use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
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
    /// Layer digests of the last manifest stored, in manifest order.
    layers: Vec<String>,
    config: Option<String>,
    /// Keyed by blob digest, or `manifests/<reference>`.
    faults: HashMap<String, Script>,
    /// Every blob `HEAD` answered with a fault.
    head_fault: Option<u16>,
    /// `GET`s per key, and `HEAD`s per blob digest.
    gets: HashMap<String, usize>,
    heads: HashMap<String, usize>,
    uploads: usize,
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

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The layer blob digests of the manifest last pushed.
    pub fn layers(&self) -> Vec<String> {
        self.state().layers.clone()
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
                lock().blobs.insert(digest.clone(), request.body);
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
        return match (blob, fault) {
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
    }

    if let Some(index) = rest.find("/manifests/") {
        let reference = rest[index + "/manifests/".len()..].to_owned();
        let key = format!("manifests/{reference}");

        if request.method == "PUT" {
            let media_type = request
                .headers
                .get("content-type")
                .cloned()
                .unwrap_or_default();
            let digest = sha256(&request.body);
            let parsed: serde_json::Value = serde_json::from_slice(&request.body).unwrap();

            let mut state = lock();
            state.layers = parsed["layers"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|layer| layer["digest"].as_str().unwrap().to_owned())
                .collect();
            state.config = parsed["config"]["digest"].as_str().map(str::to_owned);
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

        let (manifest, fault) = {
            let mut state = lock();
            *state.gets.entry(key.clone()).or_default() += 1;
            let fault = state.faults.get_mut(&key).and_then(Script::next);
            (state.manifests.get(&reference).cloned(), fault)
        };

        return match (manifest, fault) {
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
