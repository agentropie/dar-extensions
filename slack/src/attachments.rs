use std::{
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use futures_util::StreamExt;
use url::Url;

use crate::api::{SlackClient, SlackError};

pub const MAX_UPLOAD_BYTES: u64 = 25 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedFile {
    pub path: PathBuf,
    pub bytes: u64,
}

pub fn safe_filename(name: &str) -> Option<&str> {
    let path = Path::new(name);
    if name.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return None;
    }
    path.file_name()?.to_str()
}

pub fn upload_path(agent_root: &Path, name: &str) -> Result<PathBuf, SlackError> {
    let filename =
        safe_filename(name).ok_or_else(|| SlackError::new("unsafe_attachment_filename", None))?;
    Ok(agent_root.join("data").join("uploads").join(filename))
}

pub async fn download_private_file(
    client: &SlackClient,
    url: &str,
    agent_root: &Path,
    filename: &str,
    max_bytes: u64,
) -> Result<DownloadedFile, SlackError> {
    let url = Url::parse(url).map_err(|_| SlackError::new("invalid_attachment_url", None))?;
    if url.scheme() != "https"
        || !url
            .host_str()
            .is_some_and(|host| host == "slack.com" || host.ends_with(".slack.com"))
    {
        return Err(SlackError::new("untrusted_attachment_url", None));
    }
    let uploads = canonical_uploads_dir(agent_root).await?;
    let filename = safe_filename(filename)
        .ok_or_else(|| SlackError::new("unsafe_attachment_filename", None))?;
    let response = client.download(url).await?;
    if response.status().is_redirection() {
        return Err(SlackError::new("attachment_redirect_rejected", None));
    }
    if !response.status().is_success() {
        return Err(SlackError::new("attachment_download_failed", None));
    }
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes)
    {
        return Err(SlackError::new("attachment_too_large", None));
    }
    let (temporary, mut file) = unique_temporary(&uploads).await?;
    let target = unique_target(&uploads, filename).await?;
    let result = async {
        let mut received = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| SlackError::new("attachment_read_failed", None))?;
            received = received
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| SlackError::new("attachment_too_large", None))?;
            if received > max_bytes {
                return Err(SlackError::new("attachment_too_large", None));
            }
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
                .await
                .map_err(|_| SlackError::new("attachment_write_failed", None))?;
        }
        tokio::io::AsyncWriteExt::flush(&mut file)
            .await
            .map_err(|_| SlackError::new("attachment_write_failed", None))?;
        drop(file);
        tokio::fs::rename(&temporary, &target)
            .await
            .map_err(|_| SlackError::new("attachment_rename_failed", None))?;
        Ok(DownloadedFile {
            path: target,
            bytes: received,
        })
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

async fn canonical_uploads_dir(agent_root: &Path) -> Result<PathBuf, SlackError> {
    let root = tokio::fs::canonicalize(agent_root)
        .await
        .map_err(|_| SlackError::new("uploads_root_not_found", None))?;
    let uploads = root.join("data").join("uploads");
    tokio::fs::create_dir_all(&uploads)
        .await
        .map_err(|_| SlackError::new("uploads_directory_failed", None))?;
    let canonical_uploads = tokio::fs::canonicalize(&uploads)
        .await
        .map_err(|_| SlackError::new("uploads_directory_failed", None))?;
    if !canonical_uploads.starts_with(&root) {
        return Err(SlackError::new("uploads_directory_escaped_root", None));
    }
    Ok(canonical_uploads)
}

async fn unique_target(directory: &Path, filename: &str) -> Result<PathBuf, SlackError> {
    for _ in 0..32 {
        let id = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("{id}-{filename}"));
        if tokio::fs::symlink_metadata(&path).await.is_err() {
            return Ok(path);
        }
    }
    Err(SlackError::new("attachment_target_collision", None))
}

async fn unique_temporary(directory: &Path) -> Result<(PathBuf, tokio::fs::File), SlackError> {
    for _ in 0..32 {
        let id = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(".slack-upload-{}-{id}.part", std::process::id()));
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(SlackError::new("attachment_write_failed", None)),
        }
    }
    Err(SlackError::new("attachment_temp_collision", None))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_path_traversal() {
        assert!(safe_filename("../secret.png").is_none());
        assert!(safe_filename("folder/image.png").is_none());
        assert!(safe_filename("/tmp/image.png").is_none());
    }
    #[test]
    fn permits_single_filename() {
        assert_eq!(safe_filename("screen shot.png"), Some("screen shot.png"));
    }
    #[test]
    fn keeps_uploads_under_agent_data() {
        assert_eq!(
            upload_path(Path::new("/agent"), "x.png").unwrap(),
            Path::new("/agent/data/uploads/x.png")
        );
    }
}
