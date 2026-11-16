//! A directory as an OCI image layout.
//!
//! ```text
//! <root>/
//!   oci-layout           {"imageLayoutVersion":"1.0.0"}
//!   index.json           an OCI image index: one manifest descriptor per tag
//!   blobs/sha256/<hex>   layers, manifests and inscriptions, by digest
//! ```
//!
//! A layout holds what a registry repository holds (decision 0080): the same
//! manifests under the same tags, over one shared `blobs/`. So it holds many
//! sequences, a seal appends one, and the two can be copied into each other
//! blob for blob. [`SnapshotDir::pull`] reads one sequence and
//! [`SnapshotDir::resolve`] walks its chain, as a registry's do.
//!
//! [`BlobStore`] is the content-addressed half on its own, with no index: a
//! blob is staged under a [`STAGING_PREFIX`] name and committed by its
//! digest. A layout writes its layers through one.
//!
//! Every blob decompresses with `zstd -d < blobs/sha256/<hex>` or reads as
//! JSON, so a snapshot stays inspectable by hand.

use std::{
    collections::BTreeMap,
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use serde::{Deserialize, Serialize};

pub use crate::transport::{BlobIndex, LayerSpec, WrittenLayer};

use crate::{
    chain::Chain,
    digest::{read_blob, undecodable, LayerDigests, LayerWriter},
    frame::{CanonicalCbor, LayerHeader, Limits, SeqReader, SeqWriter},
    inscription::{canonical_json, LayerDescriptor},
    layer::{check_header, check_identity, check_record_count, LayerReader},
    manifest::{
        build_manifests, check_envelope, inscription_of, manifest_bytes, read_manifest, Array,
        Descriptor, Manifest, Manifests, IMAGE_MANIFEST_MEDIA_TYPE,
    },
    profile::{checked_tag_for_sequence, tip_tag, validate_tag, Profile},
    transport::{open_layer, RecordSink, SnapshotReader, SnapshotWriter},
    Digest, Error, Inscription, ARTIFACT_TYPE,
};

/// The file that marks a directory as an OCI image layout.
pub const LAYOUT_FILE: &str = "oci-layout";

/// The `imageLayoutVersion` this implementation writes and reads.
pub const LAYOUT_VERSION: &str = "1.0.0";

/// The image index naming a layout's tags.
pub const INDEX_FILE: &str = "index.json";

/// Directory holding content-addressed blobs: `blobs/<algorithm>/<hex>`.
pub const BLOBS_DIR: &str = "blobs";

/// Prefix of a file being written before it has its name: a staged blob in
/// `blobs/`, or a document at the root of a layout.
pub const STAGING_PREFIX: &str = ".staging-";

/// Media type of an OCI image index, the document `index.json` is.
pub const IMAGE_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";

/// Annotation naming the tag of an `index.json` entry.
pub const REF_NAME_ANNOTATION: &str = "org.opencontainers.image.ref.name";

/// A layer read back from disk, verified against its descriptor.
pub struct Layer {
    header: LayerHeader,
    content: Vec<u8>,
    header_len: usize,
    digests: LayerDigests,
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layer")
            .field("header", &self.header)
            .field("digests", &self.digests)
            .finish_non_exhaustive()
    }
}

impl Layer {
    pub fn header(&self) -> &LayerHeader {
        &self.header
    }

    pub fn digests(&self) -> &LayerDigests {
        &self.digests
    }

    /// The profile's content records, header excluded. Each is validated
    /// canonical as it is yielded.
    pub fn records(&self) -> SeqReader<'_> {
        SeqReader::new(&self.content[self.header_len..])
    }

    /// The encoded header record, as it appears at the head of the sequence.
    pub fn header_bytes(&self) -> &[u8] {
        &self.content[..self.header_len]
    }

    /// The whole uncompressed sequence, header record included. This is exactly
    /// the byte string the `diffId` covers.
    pub fn as_bytes(&self) -> &[u8] {
        &self.content
    }
}

/// A content-addressed blob store: `blobs/sha256/<hex>` under a root.
///
/// A blob is written to a [`Staged`] file and committed by its digest, so a
/// digest's name never holds part of a blob. The store knows no tags;
/// [`SnapshotDir`] is the layout over one. One writer at a time.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Open `root` as a blob store, creating `blobs/sha256` if it is absent.
    pub fn create(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();

        fs::create_dir_all(root.join(BLOBS_DIR).join(Digest::ALGORITHM))?;

        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where the blob of `digest` lives, whether or not it is there.
    pub fn blob_path(&self, digest: &Digest) -> PathBuf {
        self.blobs_dir().join(digest.to_hex())
    }

    fn blobs_dir(&self) -> PathBuf {
        self.root.join(BLOBS_DIR).join(Digest::ALGORITHM)
    }

    /// A new staging file in `blobs/`, beside `sha256/` and never in it.
    pub fn stage(&self) -> Result<Staged, Error> {
        static STAGED: AtomicU64 = AtomicU64::new(0);

        let path = self.root.join(BLOBS_DIR).join(format!(
            "{STAGING_PREFIX}{}-{}",
            std::process::id(),
            STAGED.fetch_add(1, Ordering::Relaxed),
        ));

        Ok(Staged {
            file: Some(fs::File::create(&path)?),
            path,
            blob_dir: self.blobs_dir(),
            committed: false,
        })
    }

    /// Store `bytes` and return their digest. A blob already there is kept.
    pub fn put(&self, bytes: &[u8]) -> Result<Digest, Error> {
        let digest = Digest::compute(bytes);

        if !self.blob_path(&digest).exists() {
            let mut staged = self.stage()?;
            staged.write_all(bytes)?;
            staged.commit(&digest)?;
        }

        Ok(digest)
    }

    /// The blob `descriptor` names, held to its digest and size, or `None`
    /// when it is not here.
    fn read(&self, descriptor: &Descriptor) -> Result<Option<Vec<u8>>, Error> {
        let digest = descriptor.digest.parse::<Digest>()?;

        let mut file = match fs::File::open(self.blob_path(&digest)) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mismatch = |actual: String| Error::DigestMismatch {
            subject: format!("blob {digest} ({} bytes claimed)", descriptor.size),
            expected: digest.to_string(),
            actual,
        };

        // The size is checked before anything is read.
        let size = file.metadata()?.len();

        if size as i64 != descriptor.size {
            return Err(mismatch(format!("{size} bytes")));
        }

        let mut raw = Vec::with_capacity(size as usize);
        io::Read::read_to_end(&mut file, &mut raw)?;

        let actual = Digest::compute(&raw);

        if actual != digest || raw.len() as u64 != size {
            return Err(mismatch(format!("{actual} ({} bytes)", raw.len())));
        }

        Ok(Some(raw))
    }

    /// Open the blob holding a layer. A layer the index does not place and a
    /// blob that is not here are one condition, [`Error::LayerNotFound`].
    fn open_layer(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<fs::File, Error> {
        let missing = || Error::LayerNotFound {
            kind: descriptor.kind.clone(),
            diff_id: descriptor.diff_id.to_string(),
        };

        let blob = index.blob_for(&descriptor.diff_id).ok_or_else(missing)?;

        fs::File::open(self.blob_path(&blob)).map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => missing(),
            _ => e.into(),
        })
    }

    fn sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
        pending: Option<Pending>,
    ) -> Result<LayerSink, Error> {
        // The profile's media type is checked before the staging file exists.
        let (sequence, media_type) = open_layer(profile, spec, level, || self.stage())?;

        Ok(LayerSink {
            sequence,
            kind: spec.kind.clone(),
            media_type,
            scope: spec.scope.clone(),
            pending,
        })
    }
}

/// A blob being written, before it has a name.
///
/// Dropped without [`Staged::commit`], it removes its file, so a write that
/// fails part way leaves nothing behind.
pub struct Staged {
    /// Closed before the file is renamed or removed.
    file: Option<fs::File>,
    path: PathBuf,
    blob_dir: PathBuf,
    committed: bool,
}

impl Staged {
    fn file(&mut self) -> &mut fs::File {
        self.file
            .as_mut()
            .expect("a staged blob holds its file until it is committed")
    }

    /// Make the staged bytes durable and name them by `digest`, their sha256,
    /// which the caller computed as it wrote them.
    ///
    /// A blob already under that name holds the same bytes: it is kept, and
    /// the staging file is removed.
    pub fn commit(mut self, digest: &Digest) -> Result<(), Error> {
        if let Some(file) = self.file.take() {
            file.sync_all()?;
        }

        let blob = self.blob_dir.join(digest.to_hex());

        if !blob.exists() {
            fs::rename(&self.path, &blob)?;
            self.committed = true;
        }

        Ok(())
    }
}

impl Write for Staged {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file().flush()
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        drop(self.file.take());

        if !self.committed {
            // Nobody is left to report a failure to, and the file is inert.
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// The layers a layout's sinks finished since its last seal.
type Pending = Arc<Mutex<Vec<WrittenLayer>>>;

/// A poisoned lock means a layer panicked mid-write; the list holds whole
/// entries, and the failed layer reports its own error.
fn lock(pending: &Pending) -> std::sync::MutexGuard<'_, Vec<WrittenLayer>> {
    pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A layer of a blob store being written, one record at a time.
///
/// Records are framed, hashed and compressed on the way past into a
/// [`Staged`] file, which [`RecordSink::finish`] commits by the blob's
/// digest. A sink owns its staging file, so several can be open at once.
pub struct LayerSink {
    sequence: SeqWriter<LayerWriter<Staged>>,
    kind: String,
    media_type: String,
    scope: serde_json::Value,
    /// The layout's list the finished layer joins, when a layout opened it.
    pending: Option<Pending>,
}

impl RecordSink for LayerSink {
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        self.sequence.write_record(record)
    }

    fn records(&self) -> u64 {
        self.sequence.count()
    }

    /// Close the frame and commit the blob by its digest. The descriptor and
    /// digests come from the record stream, so a blob that was already there
    /// gives the same answer.
    fn finish(self) -> Result<WrittenLayer, Error> {
        let count = self.sequence.count();
        let (staged, digests) = self.sequence.into_inner().finish()?;

        staged.commit(&digests.blob_digest)?;

        let written = WrittenLayer {
            descriptor: LayerDescriptor {
                kind: self.kind,
                media_type: self.media_type,
                diff_id: digests.diff_id,
                records: count,
                uncompressed_size: digests.uncompressed_size,
                scope: self.scope,
            },
            digests,
        };

        if let Some(pending) = &self.pending {
            lock(pending).push(written.clone());
        }

        Ok(written)
    }
}

/// A blob store on its own: layers go into it, and a seal stores the
/// inscription as one more blob and tags nothing.
impl SnapshotWriter for BlobStore {
    type Sink = LayerSink;

    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<LayerSink, Error> {
        self.sink(profile, spec, level, None)
    }

    fn seal(&self, _profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        self.put(&inscription.canonicalize()?)
    }
}

/// `oci-layout`'s document.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LayoutFile {
    image_layout_version: String,
}

/// `index.json`'s document: an OCI image index. Unknown fields are ignored.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImageIndex {
    schema_version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    media_type: Option<String>,
    manifests: Vec<Descriptor>,
}

/// The `index.json` entry of a manifest whose bytes are `body`, before its tag
/// is set.
fn manifest_entry(body: &[u8]) -> Descriptor {
    Descriptor {
        media_type: IMAGE_MANIFEST_MEDIA_TYPE.to_owned(),
        digest: Digest::compute(body).to_string(),
        size: body.len() as i64,
        artifact_type: Some(ARTIFACT_TYPE.to_owned()),
        ..Default::default()
    }
}

/// Write `name` under `dir` through a staging file and a rename, so the name
/// never holds part of a document.
fn replace(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), Error> {
    let staging = dir.join(format!("{STAGING_PREFIX}{name}-{}", std::process::id()));

    let written = (|| {
        let mut file = fs::File::create(&staging)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staging, dir.join(name))
    })();

    if written.is_err() {
        let _ = fs::remove_file(&staging);
    }

    Ok(written?)
}

/// Make the entries of `dir` durable, so a document written next never names
/// a blob a crash could lose.
fn sync_dir(dir: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    fs::File::open(dir)?.sync_all()?;

    #[cfg(not(unix))]
    let _ = dir;

    Ok(())
}

/// A directory holding an OCI image layout: a snapshot repository on disk.
///
/// Layers go into its [`BlobStore`]. A seal writes the inscription and the
/// manifests beside them and tags the sequence in `index.json`, so sealing
/// sequence N into a layout that holds N − 1 appends to it. One writer at a
/// time.
#[derive(Debug)]
pub struct SnapshotDir {
    blobs: BlobStore,
    pending: Pending,
}

impl SnapshotDir {
    /// Open the layout at `root` to write into it, creating it if absent.
    ///
    /// A layout already there is kept as it is, and a seal appends to it.
    pub fn create(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();
        let existing = root.join(LAYOUT_FILE).try_exists()?;

        if existing {
            check_layout(&root)?;
        }

        let blobs = BlobStore::create(root)?;

        if !existing {
            let layout = LayoutFile {
                image_layout_version: LAYOUT_VERSION.to_owned(),
            };
            replace(&blobs.root, LAYOUT_FILE, &serde_json::to_vec(&layout)?)?;
        }

        if !blobs.root.join(INDEX_FILE).try_exists()? {
            write_index(&blobs.root, &BTreeMap::new())?;
        }

        Ok(Self {
            blobs,
            pending: Pending::default(),
        })
    }

    /// Open an existing layout.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let blobs = BlobStore { root: root.into() };

        check_layout(&blobs.root)?;

        Ok(Self {
            blobs,
            pending: Pending::default(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.blobs.root
    }

    /// The content-addressed store under this layout.
    pub fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    pub fn blob_path(&self, digest: &Digest) -> PathBuf {
        self.blobs.blob_path(digest)
    }

    /// Carry a layer whose blob this layout already holds into the next seal,
    /// as a publish resuming an interrupted attempt does.
    ///
    /// `Ok(false)` when the blob is not here at the size `layer` records:
    /// nothing is carried, and the caller builds the layer after all.
    pub fn adopt_carried(&self, layer: WrittenLayer) -> Result<bool, Error> {
        match fs::metadata(self.blob_path(&layer.digests.blob_digest)) {
            Ok(metadata) if metadata.len() == layer.digests.compressed_size => {}
            Ok(_) => return Ok(false),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        }

        lock(&self.pending).push(layer);

        Ok(true)
    }

    /// Every tag `index.json` holds, with the manifest descriptor it names.
    pub fn tags(&self) -> Result<BTreeMap<String, Descriptor>, Error> {
        read_index(self.root())
    }

    /// Read the sequence `tag` names: its manifests and inscription, and so
    /// its own layers only. [`SnapshotDir::resolve`] reads the whole
    /// snapshot.
    ///
    /// A tag the layout does not hold is [`Error::NoSuchTag`]. A head whose
    /// tip manifest is not here is [`Error::TipGone`].
    pub fn pull(&self, profile: &dyn Profile, tag: &str) -> Result<Snapshot, Error> {
        validate_tag(tag)?;
        self.pull_from(&self.tags()?, profile, tag)
    }

    /// Pull the immutable tag `profile` renders for `sequence`.
    pub fn pull_sequence(&self, profile: &dyn Profile, sequence: u64) -> Result<Snapshot, Error> {
        self.pull(profile, &checked_tag_for_sequence(profile, sequence)?)
    }

    /// Pull the profile's moving tag.
    pub fn pull_latest(&self, profile: &dyn Profile) -> Result<Snapshot, Error> {
        self.pull(profile, profile.moving_tag())
    }

    /// Resolve `tag` into the whole snapshot: the sequence it names, read as
    /// [`SnapshotDir::pull`] reads it, and every inscription its links reach
    /// ([`Chain::walk`]). A link whose diff manifest or inscription the
    /// layout does not hold is [`Error::MissingLink`].
    pub fn resolve(&self, profile: &dyn Profile, tag: &str) -> Result<Snapshot, Error> {
        validate_tag(tag)?;

        let tags = self.tags()?;
        let mut snapshot = self.pull_from(&tags, profile, tag)?;
        let mut linked_blobs = BlobIndex::default();

        let chain = Chain::walk(profile, snapshot.inscription.clone(), |link| {
            let tag = checked_tag_for_sequence(profile, link.sequence)?;

            let Some(found) = tags.get(&tag) else {
                return Ok(None);
            };

            let Some((manifest, inscription)) = self.linked(found)? else {
                return Ok(None);
            };

            linked_blobs.extend(read_manifest(&manifest, &inscription, Array::Diff)?);

            Ok(Some(inscription))
        })?;

        snapshot.index.extend(linked_blobs);
        snapshot.chain = Some(chain);

        Ok(snapshot)
    }

    /// Resolve the immutable tag `profile` renders for `sequence`.
    pub fn resolve_sequence(
        &self,
        profile: &dyn Profile,
        sequence: u64,
    ) -> Result<Snapshot, Error> {
        self.resolve(profile, &checked_tag_for_sequence(profile, sequence)?)
    }

    /// Resolve the profile's moving tag: the newest snapshot, whole.
    pub fn resolve_latest(&self, profile: &dyn Profile) -> Result<Snapshot, Error> {
        self.resolve(profile, profile.moving_tag())
    }

    fn pull_from(
        &self,
        tags: &BTreeMap<String, Descriptor>,
        profile: &dyn Profile,
        tag: &str,
    ) -> Result<Snapshot, Error> {
        let found = tags.get(tag).ok_or_else(|| Error::NoSuchTag {
            tag: tag.to_owned(),
        })?;

        let (diff, inscription) = self.linked(found)?.ok_or_else(|| {
            Error::Layout(format!(
                "{INDEX_FILE} tags {tag:?} as {}, and its manifest or inscription is not in \
                 {BLOBS_DIR}/",
                found.digest,
            ))
        })?;

        inscription.check_profile(profile)?;

        let mut index = read_manifest(&diff, &inscription, Array::Diff)?;

        let tip = if inscription.tip.is_empty() {
            None
        } else {
            let tag = tip_tag(&checked_tag_for_sequence(profile, inscription.sequence)?)?;

            let tip = match tags.get(&tag) {
                Some(found) => self.manifest(found)?,
                None => None,
            };

            let Some(tip) = tip else {
                return Err(Error::TipGone {
                    sequence: inscription.sequence,
                    tag,
                });
            };

            index.extend(read_manifest(&tip, &inscription, Array::Tip)?);

            Some(tip)
        };

        Ok(Snapshot {
            blobs: self.blobs.clone(),
            manifests: Manifests { diff, tip },
            inscription,
            index,
            chain: None,
        })
    }

    /// The manifest an `index.json` entry names, or `None` when its blob is not
    /// here.
    fn manifest(&self, entry: &Descriptor) -> Result<Option<Manifest>, Error> {
        if entry.media_type != IMAGE_MANIFEST_MEDIA_TYPE {
            return Err(Error::Layout(format!(
                "{INDEX_FILE} names {} as {:?}, not an image manifest",
                entry.digest, entry.media_type,
            )));
        }

        let Some(raw) = self.blobs.read(entry)? else {
            return Ok(None);
        };

        let manifest: Manifest = serde_json::from_slice(&raw)?;

        check_envelope(&manifest)?;

        Ok(Some(manifest))
    }

    /// A diff manifest and the inscription it carries, held to each other, or
    /// `None` when either blob is not here.
    fn linked(&self, entry: &Descriptor) -> Result<Option<(Manifest, Inscription)>, Error> {
        let Some(manifest) = self.manifest(entry)? else {
            return Ok(None);
        };

        let Some(raw) = self.blobs.read(&manifest.config)? else {
            return Ok(None);
        };

        let inscription = inscription_of(&manifest, &raw)?;

        Ok(Some((manifest, inscription)))
    }
}

/// Refuse a directory that is not a layout of the version this reads.
fn check_layout(root: &Path) -> Result<(), Error> {
    let raw = fs::read(root.join(LAYOUT_FILE)).map_err(|e| match e.kind() {
        io::ErrorKind::NotFound => Error::Layout(format!(
            "no {LAYOUT_FILE} in {}, so it is not an OCI image layout",
            root.display()
        )),
        _ => e.into(),
    })?;

    let layout: LayoutFile = serde_json::from_slice(&raw)?;

    if layout.image_layout_version != LAYOUT_VERSION {
        return Err(Error::Layout(format!(
            "{LAYOUT_FILE} declares imageLayoutVersion {:?}; this implementation reads \
             {LAYOUT_VERSION:?}",
            layout.image_layout_version,
        )));
    }

    Ok(())
}

/// `index.json` as tag → manifest descriptor. Entries without a tag are
/// skipped; a tag named twice is refused.
fn read_index(root: &Path) -> Result<BTreeMap<String, Descriptor>, Error> {
    let index: ImageIndex = serde_json::from_slice(&fs::read(root.join(INDEX_FILE))?)?;

    if index.schema_version != 2 {
        return Err(Error::Layout(format!(
            "{INDEX_FILE} has schemaVersion {}, not 2",
            index.schema_version,
        )));
    }

    let mut tags = BTreeMap::new();

    for entry in index.manifests {
        let Some(tag) = entry
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(REF_NAME_ANNOTATION))
            .cloned()
        else {
            continue;
        };

        if tags.insert(tag.clone(), entry).is_some() {
            return Err(Error::Layout(format!("{INDEX_FILE} names {tag:?} twice")));
        }
    }

    Ok(tags)
}

/// Write `tags` as `index.json`: canonical JSON, entries in tag order.
fn write_index(root: &Path, tags: &BTreeMap<String, Descriptor>) -> Result<(), Error> {
    let manifests = tags
        .iter()
        .map(|(tag, entry)| {
            let mut entry = entry.clone();
            entry
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .insert(REF_NAME_ANNOTATION.to_owned(), tag.clone());
            entry
        })
        .collect();

    let index = ImageIndex {
        schema_version: 2,
        media_type: Some(IMAGE_INDEX_MEDIA_TYPE.to_owned()),
        manifests,
    };

    replace(
        root,
        INDEX_FILE,
        &canonical_json(&serde_json::to_value(&index)?)?,
    )
}

impl SnapshotWriter for SnapshotDir {
    type Sink = LayerSink;

    /// Open a layer in this layout's blob store. The next seal lists it.
    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<LayerSink, Error> {
        self.blobs
            .sink(profile, spec, level, Some(Arc::clone(&self.pending)))
    }

    /// Write the inscription and manifests, then tag them, as a registry
    /// publish does: the inscription blob, the tip manifest under
    /// `<tag>.tip`, the diff manifest under `<tag>`, and `latest` moved to it.
    ///
    /// The three tags land in one rename of `index.json`, after every blob
    /// they name is durable. A seal lists the layers finished since the last
    /// one, and consumes them only when it succeeds. A sequence's tags are
    /// written once: one that names another manifest is
    /// [`Error::TagTaken`], and sealing the same manifests again changes
    /// nothing.
    fn seal(&self, profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        let sequence_tag = checked_tag_for_sequence(profile, inscription.sequence)?;
        let tip_tag = tip_tag(&sequence_tag)?;
        let moving_tag = profile.moving_tag().to_owned();
        validate_tag(&moving_tag)?;

        let (manifests, config) = build_manifests(inscription, &lock(&self.pending))?;
        let diff = manifest_bytes(&manifests.diff)?;
        let tip = manifests.tip.as_ref().map(manifest_bytes).transpose()?;

        let mut tags = self.tags()?;
        let mut tagging = Vec::new();

        if let Some(tip) = &tip {
            tagging.push((tip_tag, manifest_entry(tip)));
        }

        tagging.push((sequence_tag, manifest_entry(&diff)));

        for (tag, entry) in &tagging {
            match tags.get(tag) {
                Some(held) if held.digest != entry.digest => {
                    return Err(Error::TagTaken {
                        tag: tag.clone(),
                        held: held.digest.clone(),
                        sealing: entry.digest.clone(),
                    })
                }
                _ => {}
            }
        }

        let identity = self.blobs.put(&config)?;

        if let Some(tip) = &tip {
            self.blobs.put(tip)?;
        }

        self.blobs.put(&diff)?;
        sync_dir(&self.blobs.blobs_dir())?;

        let latest = manifest_entry(&diff);
        tags.extend(tagging);
        tags.insert(moving_tag, latest);

        write_index(self.root(), &tags)?;
        sync_dir(self.root())?;

        lock(&self.pending).clear();

        Ok(identity)
    }
}

/// A snapshot read from a layout: one sequence after [`SnapshotDir::pull`],
/// the whole chain after [`SnapshotDir::resolve`].
///
/// [`SnapshotReader::read_inscription`] is the head's inscription.
pub struct Snapshot {
    blobs: BlobStore,
    manifests: Manifests,
    inscription: Inscription,
    index: BlobIndex,
    chain: Option<Chain>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("root", &self.blobs.root)
            .field("sequence", &self.inscription.sequence)
            .field("resolved", &self.chain.is_some())
            .finish_non_exhaustive()
    }
}

impl Snapshot {
    /// The head's diff manifest, the one its tag names.
    pub fn manifest(&self) -> &Manifest {
        &self.manifests.diff
    }

    /// The head's tip manifest, or `None` when its `tip` is empty.
    pub fn tip_manifest(&self) -> Option<&Manifest> {
        self.manifests.tip.as_ref()
    }

    /// The chain this snapshot was resolved through, or `None` after a pull.
    pub fn chain(&self) -> Option<&Chain> {
        self.chain.as_ref()
    }

    /// Read one layer whole, verifying it against its descriptor and its own
    /// header: identity, uncompressed size, record count, profile and kind.
    ///
    /// The layer is held in full, which makes this a fixture's read;
    /// [`SnapshotReader::stream_layer`] checks the same claims out of a
    /// bounded window.
    pub fn read_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
    ) -> Result<Layer, Error> {
        // The descriptor's claim is the ceiling on decompression.
        let (content, digests) = read_blob(
            self.blobs.open_layer(index, descriptor)?,
            descriptor.uncompressed_size,
        )
        .map_err(|error| match error {
            // The refusal the streaming reader gives the same bytes.
            Error::Io(io) if undecodable(&io).is_some() => Error::LayerMismatch {
                kind: descriptor.kind.clone(),
                reason: io.to_string(),
            },
            other => other,
        })?;

        check_identity(&digests, descriptor)?;

        let header_len = match SeqReader::new(&content).next() {
            Some(Ok(record)) => record.len(),
            Some(Err(e)) => return Err(e),
            None => {
                return Err(Error::LayerMismatch {
                    kind: descriptor.kind.clone(),
                    reason: "layer is empty; every layer starts with a header record".to_owned(),
                })
            }
        };

        let header = LayerHeader::decode(&content[..header_len])?;

        check_header(&header, profile, descriptor)?;

        // Counted by iterating: `SeqReader` yields a malformed record as one
        // `Err` and ends, which `count()` would tally as a record.
        let mut records = 1u64;

        for record in SeqReader::new(&content[header_len..]) {
            record?;
            records += 1;
        }

        check_record_count(records, descriptor)?;

        Ok(Layer {
            header,
            content,
            header_len,
            digests,
        })
    }
}

impl SnapshotReader for Snapshot {
    type Blob = fs::File;

    fn read_inscription(&self) -> Result<Inscription, Error> {
        Ok(self.inscription.clone())
    }

    fn blob_index(&self) -> Result<BlobIndex, Error> {
        Ok(self.index.clone())
    }

    /// The length of the blob file: one `stat`, nothing read. `None` for a
    /// layer the index does not place or whose blob is not here.
    fn compressed_size(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<Option<u64>, Error> {
        let Some(blob) = index.blob_for(&descriptor.diff_id) else {
            return Ok(None);
        };

        match fs::metadata(self.blobs.blob_path(&blob)) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<fs::File>, Error> {
        LayerReader::new(
            self.blobs.open_layer(index, descriptor)?,
            profile,
            descriptor,
            limits,
        )
    }
}
