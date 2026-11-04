//! Advisory locks shared by independent MCP bridge processes.
use std::path::Path;

use crate::Failure;
use tokio::time::Instant;

#[derive(Debug)]
pub struct FileLock(std::fs::File);

impl FileLock {
    pub async fn acquire(path: &Path, deadline: Instant) -> Result<Self, Failure> {
        reject_symlink(path).await?;
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(path)
            .await
            .map_err(|_| Failure::new("model_unavailable", "Cannot open transcription lock"))?
            .into_std()
            .await;
        loop {
            if Instant::now() >= deadline {
                return Err(Failure::new("busy", "Transcription is busy; try again"));
            }
            match fs2::FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(Self(file)),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(Failure::new("busy", "Transcription is busy; try again"));
                    }
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + std::time::Duration::from_millis(20)),
                    )
                    .await;
                }
                Err(_) => {
                    return Err(Failure::new(
                        "model_unavailable",
                        "Cannot lock transcription files",
                    ));
                }
            }
        }
    }
}

pub(crate) async fn reject_symlink(path: &Path) -> Result<(), Failure> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Failure::new(
            "model_unavailable",
            "Transcription storage cannot contain symlinks",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Failure::new(
            "model_unavailable",
            "Cannot inspect transcription storage",
        )),
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn independent_processes_serialize_access() {
        if let Some(path) = std::env::var_os("TRANSCRIBE_LOCK_TEST_CHILD") {
            let path = std::path::PathBuf::from(path);
            let _lock = FileLock::acquire(&path, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            tokio::fs::write(path.with_extension("acquired"), b"acquired")
                .await
                .unwrap();
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transcribe.lock");
        let first = FileLock::acquire(&path, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lock::tests::independent_processes_serialize_access",
            ])
            .env("TRANSCRIBE_LOCK_TEST_CHILD", &path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!path.with_extension("acquired").exists());
        assert!(child.try_wait().unwrap().is_none());
        drop(first);
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            tokio::fs::read(path.with_extension("acquired"))
                .await
                .unwrap(),
            b"acquired"
        );
    }

    #[tokio::test]
    async fn an_expired_deadline_never_acquires_a_free_lock() {
        let dir = tempfile::tempdir().unwrap();
        let result = FileLock::acquire(&dir.path().join("lock"), Instant::now()).await;
        assert_eq!(result.unwrap_err().code, "busy");
    }

    #[tokio::test]
    async fn a_second_holder_waits_until_the_first_releases_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transcribe.lock");
        let first = FileLock::acquire(&path, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let path_for_task = path.clone();
        let next = tokio::spawn(async move {
            FileLock::acquire(&path_for_task, Instant::now() + Duration::from_secs(2)).await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!next.is_finished());
        drop(first);
        next.await.unwrap().unwrap();
    }
}
