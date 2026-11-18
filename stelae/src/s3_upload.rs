//! Opt-in direct uploads into Zot's non-deduplicated OCI blob layout.
//! Credentials are supplied by the host, never discovered here. The OCI
//! registry remains responsible for validating and committing manifests/tags.
use crate::{Digest, Error};
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore, ObjectStoreExt};
use std::{fs::File, io::Read};

/// Explicit storage mapping. `prefix` is the actual physical S3 key prefix,
/// including any prefix duplication imposed by the registry's storage driver.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DirectS3Config(<redacted>)")
    }
}
/// The largest blob [`Upload::copy_from`] moves: one `CopyObject`'s limit.
pub const COPY_MAX: u64 = 5 * 1024 * 1024 * 1024;

pub(crate) struct Upload {
    store: std::sync::Arc<dyn ObjectStore>,
    /// The configured prefix, with a trailing slash when it is not empty.
    root: String,
    /// This repository's blob keys: `{root}{repository}/blobs/sha256/`.
    prefix: String,
}
fn error(e: impl std::fmt::Display) -> Error {
    std::io::Error::other(format!("direct S3 upload: {e}")).into()
}
fn blobs_of(root: &str, repository: &str) -> String {
    format!("{root}{repository}/blobs/sha256/")
}
impl Upload {
    pub(crate) fn new(c: Config, repository: &str) -> Result<Self, Error> {
        if !c.endpoint.starts_with("https://")
            || c.endpoint.contains('?')
            || c.endpoint.contains('@')
        {
            return Err(error("endpoint must be HTTPS without credentials or query"));
        }
        if c.prefix.split('/').any(|s| s == ".." || s == ".") {
            return Err(error("invalid storage prefix"));
        }
        let store = AmazonS3Builder::new()
            .with_endpoint(c.endpoint)
            .with_bucket_name(c.bucket)
            .with_region(c.region)
            .with_access_key_id(c.access_key_id)
            .with_secret_access_key(c.secret_access_key)
            .build()
            .map_err(error)?;
        let root = if c.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", c.prefix.trim_matches('/'))
        };
        Ok(Self {
            store: std::sync::Arc::new(store),
            prefix: blobs_of(&root, repository),
            root,
        })
    }
    fn key(&self, digest: &Digest) -> Path {
        Path::from(format!("{}{}", self.prefix, digest.to_hex()))
    }
    /// Copy the blob `digest` from `repository`'s keys to this repository's,
    /// inside the bucket, unless it is here already. Only this repository's
    /// key is written, and only once the source holds the blob at `size`.
    pub(crate) async fn copy_from(
        &self,
        repository: &str,
        digest: &Digest,
        size: u64,
    ) -> Result<(), Error> {
        let from = Path::from(format!(
            "{}{}",
            blobs_of(&self.root, repository),
            digest.to_hex()
        ));
        let to = self.key(digest);
        if from == to {
            return Err(error("a blob is copied from another repository's keys"));
        }
        match self.store.head(&from).await {
            Ok(meta) if meta.size == size => {}
            Ok(meta) => {
                return Err(error(format!(
                    "{repository} holds {digest} at {} bytes, not {size}",
                    meta.size
                )))
            }
            Err(object_store::Error::NotFound { .. }) => {
                return Err(error(format!("{repository} holds no blob {digest}")))
            }
            Err(e) => return Err(error(e)),
        }
        if self.exists(digest, size).await? {
            return Ok(());
        }
        self.store.copy(&from, &to).await.map_err(error)?;
        match self.exists(digest, size).await? {
            true => Ok(()),
            false => Err(error(format!("the copy of {digest} is not in place"))),
        }
    }
    pub(crate) async fn exists(&self, digest: &Digest, size: u64) -> Result<bool, Error> {
        match self.store.head(&self.key(digest)).await {
            Ok(meta) if meta.size == size => Ok(true),
            Ok(_) => Err(error("existing content-addressed blob has wrong size")),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(error(e)),
        }
    }
    pub(crate) async fn bytes(&self, digest: &Digest, bytes: Vec<u8>) -> Result<(), Error> {
        if Digest::compute(&bytes) != *digest {
            return Err(error("blob digest mismatch"));
        }
        if !self.exists(digest, bytes.len() as u64).await? {
            self.store
                .put(&self.key(digest), bytes.into())
                .await
                .map_err(error)?;
        }
        Ok(())
    }
    pub(crate) async fn file(
        &self,
        digest: &Digest,
        mut file: File,
        size: u64,
    ) -> Result<(), Error> {
        // Hash before publishing to the final key. Reuse the staged file and
        // fixed-size reads; a layer never becomes a resident whole-file buffer.
        use sha2::{Digest as _, Sha256};
        use std::io::{Seek, SeekFrom};
        let mut hash = Sha256::new();
        let mut buf = vec![0; 8 * 1024 * 1024];
        let mut read = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
            read += n as u64;
        }
        if read != size || format!("sha256:{}", hex::encode(hash.finalize())) != digest.to_string()
        {
            return Err(error("staged blob digest or size mismatch"));
        }
        file.seek(SeekFrom::Start(0))?;
        if size <= buf.len() as u64 {
            file.read_exact(&mut buf[..size as usize])?;
            return self.bytes(digest, buf[..size as usize].to_vec()).await;
        }
        let mut upload = self
            .store
            .put_multipart(&self.key(digest))
            .await
            .map_err(error)?;
        let result = async {
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                upload
                    .put_part(bytes::Bytes::copy_from_slice(&buf[..n]).into())
                    .await
                    .map_err(error)?;
            }
            upload.complete().await.map_err(error)?;
            Ok::<(), Error>(())
        }
        .await;
        if result.is_err() {
            let _ = upload.abort().await;
        }
        result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uploader() -> Upload {
        in_store(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "cardano/preprod",
        )
    }
    fn in_store(store: std::sync::Arc<dyn ObjectStore>, repository: &str) -> Upload {
        Upload {
            store,
            root: "scope/".into(),
            prefix: blobs_of("scope/", repository),
        }
    }
    #[test]
    fn a_copy_writes_only_the_target_key_and_only_what_the_source_holds() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store: std::sync::Arc<dyn ObjectStore> =
                std::sync::Arc::new(object_store::memory::InMemory::new());
            let (source, target) = (
                in_store(store.clone(), "cardano/preprod"),
                in_store(store.clone(), "cardano/preprod/dolos"),
            );
            let bytes = b"a layer of the old lineage".to_vec();
            let digest = Digest::compute(&bytes);
            let size = bytes.len() as u64;
            source.bytes(&digest, bytes.clone()).await.unwrap();

            target
                .copy_from("cardano/preprod", &digest, size)
                .await
                .unwrap();
            let copied = store.get(&target.key(&digest)).await.unwrap();
            assert_eq!(copied.bytes().await.unwrap().as_ref(), bytes.as_slice());
            assert_eq!(
                target.key(&digest).as_ref(),
                format!(
                    "scope/cardano/preprod/dolos/blobs/sha256/{}",
                    digest.to_hex()
                )
            );
            // Again, with the blob in place: nothing to do.
            target
                .copy_from("cardano/preprod", &digest, size)
                .await
                .unwrap();

            let absent = Digest::compute(b"never published");
            assert!(target
                .copy_from("cardano/preprod", &absent, 15)
                .await
                .is_err());
            assert!(!target.exists(&absent, 15).await.unwrap());
            let other = Digest::compute(b"another layer");
            source
                .bytes(&other, b"another layer".to_vec())
                .await
                .unwrap();
            assert!(target
                .copy_from("cardano/preprod", &other, 99)
                .await
                .is_err());
            assert!(!target.exists(&other, 13).await.unwrap());
            assert!(source
                .copy_from("cardano/preprod", &digest, size)
                .await
                .is_err());

            let listed: Vec<String> = futures_util::StreamExt::collect::<Vec<_>>(store.list(None))
                .await
                .into_iter()
                .map(|meta| meta.unwrap().location.to_string())
                .collect();
            assert_eq!(listed.len(), 3, "{listed:?}");
        });
    }
    #[test]
    fn direct_small_and_multipart_uploads_preserve_bytes_and_refuse_corruption() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let upload = uploader();
            for size in [1024, 9 * 1024 * 1024] {
                let bytes = vec![42; size];
                let digest = Digest::compute(&bytes);
                let mut file = tempfile::tempfile().unwrap();
                std::io::Write::write_all(&mut file, &bytes).unwrap();
                std::io::Seek::rewind(&mut file).unwrap();
                upload.file(&digest, file, size as u64).await.unwrap();
                assert!(upload.exists(&digest, size as u64).await.unwrap());
                let stored = upload
                    .store
                    .get(&upload.key(&digest))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(stored.as_ref(), bytes.as_slice());
                assert!(upload.exists(&digest, size as u64 + 1).await.is_err());
            }
            let digest = Digest::compute(b"correct");
            assert!(upload.bytes(&digest, b"wrong".to_vec()).await.is_err());
            assert!(!upload.exists(&digest, 7).await.unwrap());
        });
    }
    #[test]
    fn credentials_are_redacted_and_unsafe_endpoints_are_rejected() {
        let config = Config {
            endpoint: "http://example.com".into(),
            bucket: "test".into(),
            prefix: "".into(),
            region: "auto".into(),
            access_key_id: "access-secret".into(),
            secret_access_key: "private-secret".into(),
        };
        assert_eq!(format!("{config:?}"), "DirectS3Config(<redacted>)");
        assert!(Upload::new(config, "cardano/preprod").is_err());
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    /// Explicit live check of the actual S3 multipart protocol, with one
    /// uniquely named blob removed after successful readback.
    #[test]
    #[ignore = "requires STELAE_TEST_DIRECT_S3_CONFIG and creates a temporary S3 object"]
    fn direct_s3_multipart_round_trip() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let path =
            std::env::var("STELAE_TEST_DIRECT_S3_CONFIG").expect("explicit S3 configuration");
        let config: Config = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let upload =
                Upload::new(config, &format!("staging/direct-multipart-{unique}")).unwrap();
            let bytes = vec![42u8; 9 * 1024 * 1024];
            let digest = Digest::compute(&bytes);
            let mut file = tempfile::tempfile().unwrap();
            std::io::Write::write_all(&mut file, &bytes).unwrap();
            std::io::Seek::rewind(&mut file).unwrap();
            upload
                .file(&digest, file, bytes.len() as u64)
                .await
                .unwrap();
            let actual = upload
                .store
                .get(&upload.key(&digest))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let matches = actual.as_ref() == bytes.as_slice();
            upload.store.delete(&upload.key(&digest)).await.unwrap();
            assert!(matches, "multipart bytes differ");
        });
    }
}
