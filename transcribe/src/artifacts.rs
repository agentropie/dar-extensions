//! Pinned, verified runtime artifacts. Audio never enters the fetcher.
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::Failure;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub struct ArtifactSpec {
    pub name: &'static str,
    pub url: &'static str,
    pub size: u64,
    pub sha256: &'static str,
    pub executable: bool,
}

#[derive(Debug)]
pub struct Artifacts {
    pub model: PathBuf,
    pub helper: PathBuf,
}

#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn fetch(&self, url: &str, destination: &Path, max_bytes: u64) -> Result<(), Failure>;
}

pub struct ArtifactStore {
    dir: PathBuf,
    fetcher: Arc<dyn Fetcher>,
    manifest: Vec<ArtifactSpec>,
    verified: tokio::sync::Mutex<HashSet<&'static str>>,
}

impl ArtifactStore {
    pub fn new(dir: PathBuf, fetcher: Arc<dyn Fetcher>) -> Self {
        Self::with_manifest(
            dir,
            fetcher,
            platform_manifest(std::env::consts::OS, std::env::consts::ARCH),
        )
    }

    pub fn with_manifest(
        dir: PathBuf,
        fetcher: Arc<dyn Fetcher>,
        manifest: Vec<ArtifactSpec>,
    ) -> Self {
        Self {
            dir,
            fetcher,
            manifest,
            verified: tokio::sync::Mutex::new(HashSet::new()),
        }
    }

    pub async fn ensure(&self) -> Result<Artifacts, Failure> {
        if self.manifest.is_empty() {
            return Err(Failure::new(
                "model_unavailable",
                "Unsupported platform: transcribe requires Linux aarch64 or x86_64",
            ));
        }
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(unavailable)?;
        let _lock = crate::lock::FileLock::acquire(
            &self.dir.join("download.lock"),
            tokio::time::Instant::now() + std::time::Duration::from_secs(300),
        )
        .await?;
        for spec in &self.manifest {
            let destination = self.dir.join(spec.name);
            crate::lock::reject_symlink(&destination).await?;
            let cached = self.verified.lock().await.contains(spec.name);
            let valid = match tokio::fs::metadata(&destination).await {
                Ok(metadata) if metadata.is_file() && metadata.len() == spec.size => {
                    cached || verify(destination.clone(), spec.size, spec.sha256).await?
                }
                _ => false,
            };
            if valid {
                #[cfg(unix)]
                if spec.executable {
                    use std::os::unix::fs::PermissionsExt;
                    let file = tokio::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW)
                        .open(&destination)
                        .await
                        .map_err(unavailable)?;
                    file.set_permissions(std::fs::Permissions::from_mode(0o755))
                        .await
                        .map_err(unavailable)?;
                }
                self.verified.lock().await.insert(spec.name);
                continue;
            }
            self.verified.lock().await.remove(spec.name);
            match tokio::fs::remove_file(&destination).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(unavailable(error)),
            }
            let part = self.dir.join(format!("{}.part", spec.name));
            crate::lock::reject_symlink(&part).await?;
            match tokio::fs::remove_file(&part).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(unavailable(error)),
            }
            let _cleanup = PartFile(part.clone());
            self.fetcher.fetch(spec.url, &part, spec.size).await?;
            if !verify(part.clone(), spec.size, spec.sha256).await? {
                return Err(Failure::new(
                    "model_unavailable",
                    "Downloaded artifact failed verification",
                ));
            }
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true);
            #[cfg(unix)]
            options.custom_flags(libc::O_NOFOLLOW);
            let file = options.open(&part).await.map_err(unavailable)?;
            #[cfg(unix)]
            if spec.executable {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(std::fs::Permissions::from_mode(0o755))
                    .await
                    .map_err(unavailable)?;
            }
            file.sync_all().await.map_err(unavailable)?;
            tokio::fs::rename(&part, &destination)
                .await
                .map_err(unavailable)?;
            self.verified.lock().await.insert(spec.name);
        }
        Ok(Artifacts {
            model: self.dir.join("whistle.cact"),
            helper: self.dir.join("needle"),
        })
    }
}

fn platform_manifest(os: &str, arch: &str) -> Vec<ArtifactSpec> {
    let (url, size, sha256) = match (os, arch) {
        ("linux", "aarch64") => (
            "https://huggingface.co/Cactus-Compute/needle3/resolve/d6c7d6a2e9655505e0f8e23339356dd1705900eb/linux-arm64/needle",
            1_439_720,
            "a7dd07f740763507edbca760dc7e7065b6da6b6bd6c3a05288ca974f14ce0fcb",
        ),
        ("linux", "x86_64") => (
            "https://huggingface.co/Cactus-Compute/needle3/resolve/d6c7d6a2e9655505e0f8e23339356dd1705900eb/linux-x86_64/needle",
            1_550_176,
            "77cd35ee75c53df8532b397e11ed7cf94a45d89a9f14f50881fd9c68ba54a2d0",
        ),
        _ => return vec![],
    };
    vec![
        ArtifactSpec {
            name: "whistle.cact",
            url: "https://huggingface.co/Cactus-Compute/whistle/resolve/741118687e1f008f06d1351a402a731274789790/whistle.cact",
            size: 16_919_407,
            sha256: "b6e02f048568ac5d01a2042556c658061e699acbc0aa2a1439f52f3d461dffeb",
            executable: false,
        },
        ArtifactSpec { name: "needle", url, size, sha256, executable: true },
    ]
}

pub struct HttpFetcher(reqwest::Client);

impl HttpFetcher {
    pub fn new() -> Result<Self, Failure> {
        reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::limited(10))
            .connect_timeout(std::time::Duration::from_secs(15))
            .timeout(std::time::Duration::from_secs(180))
            .build()
            .map(Self)
            .map_err(unavailable)
    }
}

#[async_trait]
impl Fetcher for HttpFetcher {
    async fn fetch(&self, url: &str, destination: &Path, max_bytes: u64) -> Result<(), Failure> {
        let mut response = self
            .0
            .get(url)
            .send()
            .await
            .map_err(unavailable)?
            .error_for_status()
            .map_err(unavailable)?;
        // Create synchronously so cancellation cannot leave a delayed open creating a stale .part.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .map_err(unavailable)?;
        let mut file = tokio::fs::File::from_std(file);
        let mut received = 0u64;
        while let Some(chunk) = response.chunk().await.map_err(unavailable)? {
            received = received
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| unavailable("download size overflow"))?;
            if received > max_bytes {
                return Err(unavailable("artifact exceeds pinned size"));
            }
            file.write_all(&chunk).await.map_err(unavailable)?;
        }
        file.flush().await.map_err(unavailable)?;
        Ok(())
    }
}

fn unavailable(error: impl std::fmt::Display) -> Failure {
    tracing::debug!(error = %error, "transcribe artifact storage failed");
    Failure::new(
        "model_unavailable",
        "Transcription artifacts are unavailable",
    )
}

struct PartFile(PathBuf);
impl Drop for PartFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn verify(path: PathBuf, size: u64, expected: &'static str) -> Result<bool, Failure> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(path).map_err(unavailable)?;
        if file.metadata().map_err(unavailable)?.len() != size {
            return Ok(false);
        }
        let mut hasher = Sha256::new();
        let mut buffer = [0; 65536];
        loop {
            let read = file.read(&mut buffer).map_err(unavailable)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hasher.finalize()) == expected)
    })
    .await
    .map_err(unavailable)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MemoryFetcher {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Fetcher for MemoryFetcher {
        async fn fetch(
            &self,
            url: &str,
            destination: &Path,
            _max_bytes: u64,
        ) -> Result<(), Failure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let data: &[u8] = if url == "memory:model" {
                b"abc"
            } else {
                b"hello"
            };
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(destination)
                .await
                .map_err(unavailable)?;
            file.write_all(data).await.map_err(unavailable)?;
            file.flush().await.map_err(unavailable)?;
            Ok(())
        }
    }

    fn manifest() -> Vec<ArtifactSpec> {
        vec![
            ArtifactSpec {
                name: "whistle.cact",
                url: "memory:model",
                size: 3,
                sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                executable: false,
            },
            ArtifactSpec {
                name: "needle",
                url: "memory:helper",
                size: 5,
                sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                executable: true,
            },
        ]
    }

    #[tokio::test]
    async fn a_download_with_the_wrong_checksum_is_never_published() {
        let dir = tempfile::tempdir().unwrap();
        let mut specs = manifest();
        specs[0].sha256 = "0000000000000000000000000000000000000000000000000000000000000000";
        let store = ArtifactStore::with_manifest(
            dir.path().to_path_buf(),
            Arc::new(MemoryFetcher {
                calls: AtomicUsize::new(0),
            }),
            specs,
        );
        assert_eq!(store.ensure().await.unwrap_err().code, "model_unavailable");
        assert!(!dir.path().join("whistle.cact").exists());
        assert!(!dir.path().join("whistle.cact.part").exists());
    }

    struct HangingFetcher(tokio::sync::Notify);
    #[async_trait]
    impl Fetcher for HangingFetcher {
        async fn fetch(
            &self,
            _url: &str,
            destination: &Path,
            _max_bytes: u64,
        ) -> Result<(), Failure> {
            tokio::fs::write(destination, b"partial").await.unwrap();
            self.0.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn cancellation_cleans_the_partial_file_and_releases_the_download_lock() {
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(HangingFetcher(tokio::sync::Notify::new()));
        let store = Arc::new(ArtifactStore::with_manifest(
            dir.path().to_path_buf(),
            fetcher.clone(),
            manifest(),
        ));
        let task = tokio::spawn(async move { store.ensure().await });
        fetcher.0.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!dir.path().join("whistle.cact.part").exists());
        let next = ArtifactStore::with_manifest(
            dir.path().to_path_buf(),
            Arc::new(MemoryFetcher {
                calls: AtomicUsize::new(0),
            }),
            manifest(),
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), next.ensure())
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(result.unwrap().model).await.unwrap(),
            b"abc"
        );
    }

    struct FlakyFetcher(AtomicUsize);
    #[async_trait]
    impl Fetcher for FlakyFetcher {
        async fn fetch(
            &self,
            url: &str,
            destination: &Path,
            max_bytes: u64,
        ) -> Result<(), Failure> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::fs::write(destination, b"partial").await.unwrap();
                return Err(Failure::new(
                    "model_unavailable",
                    "Simulated offline download",
                ));
            }
            MemoryFetcher {
                calls: AtomicUsize::new(0),
            }
            .fetch(url, destination, max_bytes)
            .await
        }
    }

    #[tokio::test]
    async fn a_failed_download_leaves_no_partial_file_and_is_retried_without_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::with_manifest(
            dir.path().to_path_buf(),
            Arc::new(FlakyFetcher(AtomicUsize::new(0))),
            manifest(),
        );
        assert_eq!(store.ensure().await.unwrap_err().code, "model_unavailable");
        assert!(!dir.path().join("whistle.cact.part").exists());
        assert!(!dir.path().join("whistle.cact").exists());
        let artifacts = store.ensure().await.unwrap();
        assert_eq!(tokio::fs::read(artifacts.model).await.unwrap(), b"abc");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_cannot_supply_artifacts_or_redirect_downloads() {
        use std::os::unix::fs::symlink;
        for name in [
            "whistle.cact",
            "needle",
            "whistle.cact.part",
            "download.lock",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let outside = tempfile::NamedTempFile::new().unwrap();
            let literal: &[u8] = if name == "needle" { b"hello" } else { b"abc" };
            tokio::fs::write(outside.path(), literal).await.unwrap();
            symlink(outside.path(), dir.path().join(name)).unwrap();
            let store = ArtifactStore::with_manifest(
                dir.path().to_path_buf(),
                Arc::new(MemoryFetcher {
                    calls: AtomicUsize::new(0),
                }),
                manifest(),
            );
            assert_eq!(store.ensure().await.unwrap_err().code, "model_unavailable");
            assert_eq!(tokio::fs::read(outside.path()).await.unwrap(), literal);
        }
    }

    struct SlowFetcher(MemoryFetcher);
    #[async_trait]
    impl Fetcher for SlowFetcher {
        async fn fetch(
            &self,
            url: &str,
            destination: &Path,
            max_bytes: u64,
        ) -> Result<(), Failure> {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.0.fetch(url, destination, max_bytes).await
        }
    }

    #[tokio::test]
    async fn independent_stores_share_one_atomic_download() {
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(SlowFetcher(MemoryFetcher {
            calls: AtomicUsize::new(0),
        }));
        let first =
            ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest());
        let second =
            ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest());
        let (a, b) = tokio::join!(first.ensure(), second.ensure());
        a.unwrap();
        b.unwrap();
        assert_eq!(fetcher.0.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn missing_truncated_and_same_size_corrupt_artifacts_are_refetched() {
        for damaged_name in ["whistle.cact", "needle"] {
            for damage in [
                None,
                Some(&b"x"[..]),
                Some(if damaged_name == "needle" {
                    &b"world"[..]
                } else {
                    &b"bad"[..]
                }),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let fetcher = Arc::new(MemoryFetcher {
                    calls: AtomicUsize::new(0),
                });
                ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest())
                    .ensure()
                    .await
                    .unwrap();
                let path = dir.path().join(damaged_name);
                if let Some(bytes) = damage {
                    tokio::fs::write(&path, bytes).await.unwrap();
                } else {
                    tokio::fs::remove_file(&path).await.unwrap();
                }
                ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest())
                    .ensure()
                    .await
                    .unwrap();
                assert_eq!(fetcher.calls.load(Ordering::SeqCst), 3);
                let expected: &[u8] = if damaged_name == "needle" {
                    b"hello"
                } else {
                    b"abc"
                };
                assert_eq!(tokio::fs::read(path).await.unwrap(), expected);
            }
        }
    }

    #[tokio::test]
    async fn a_partial_download_left_by_a_crash_is_replaced_on_the_next_attempt() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("whistle.cact.part"), b"partial")
            .await
            .unwrap();
        let store = ArtifactStore::with_manifest(
            dir.path().to_path_buf(),
            Arc::new(MemoryFetcher {
                calls: AtomicUsize::new(0),
            }),
            manifest(),
        );
        let artifacts = store.ensure().await.unwrap();
        assert_eq!(tokio::fs::read(artifacts.model).await.unwrap(), b"abc");
        assert!(!dir.path().join("whistle.cact.part").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cached_helper_with_lost_execute_permissions_is_repaired_without_downloading() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(MemoryFetcher {
            calls: AtomicUsize::new(0),
        });
        let store =
            ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest());
        let artifacts = store.ensure().await.unwrap();
        tokio::fs::set_permissions(&artifacts.helper, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();
        let reused = store.ensure().await.unwrap();
        let permissions = tokio::fs::metadata(reused.helper)
            .await
            .unwrap()
            .permissions();
        assert_eq!(permissions.mode() & 0o777, 0o755);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn downloaded_helper_can_be_executed_from_the_model_directory() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(MemoryFetcher {
            calls: AtomicUsize::new(0),
        });
        let store = ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher, manifest());
        let artifacts = store.ensure().await.unwrap();
        let permissions = tokio::fs::metadata(artifacts.helper)
            .await
            .unwrap()
            .permissions();
        assert_eq!(permissions.mode() & 0o777, 0o755);
    }

    #[tokio::test]
    async fn an_unsupported_platform_returns_an_error_without_fetching() {
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(MemoryFetcher {
            calls: AtomicUsize::new(0),
        });
        let store = ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), vec![]);
        let error = store.ensure().await.unwrap_err();
        assert_eq!(error.code, "model_unavailable");
        assert!(error.message.contains("Unsupported platform"));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_fresh_process_reuses_valid_artifacts_without_downloading() {
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(MemoryFetcher {
            calls: AtomicUsize::new(0),
        });
        let first =
            ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest());
        first.ensure().await.unwrap();
        let restarted =
            ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher.clone(), manifest());
        restarted.ensure().await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn downloads_and_returns_verified_artifacts_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let fetcher = Arc::new(MemoryFetcher {
            calls: AtomicUsize::new(0),
        });
        let store = ArtifactStore::with_manifest(dir.path().to_path_buf(), fetcher, manifest());
        let artifacts = store.ensure().await.unwrap();
        assert_eq!(tokio::fs::read(artifacts.model).await.unwrap(), b"abc");
        assert_eq!(tokio::fs::read(artifacts.helper).await.unwrap(), b"hello");
    }
}
