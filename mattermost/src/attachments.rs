use crate::api::MattermostClient;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::io::AsyncWriteExt;

pub const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct PartialUpload(Option<PathBuf>);
impl Drop for PartialUpload {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub async fn prompt(
    client: &MattermostClient,
    root: &Path,
    file_ids: &[String],
    mut text: String,
) -> Result<String> {
    let root = tokio::fs::canonicalize(root)
        .await
        .context("attachment root unavailable")?;
    for id in file_ids.iter().take(10) {
        let info = client.file_info(id).await?;
        if info.size > MAX_ATTACHMENT_BYTES {
            anyhow::bail!("Mattermost attachment exceeds 25 MiB");
        }
        let path = download(client, &root, id, &info.name).await?;
        let relative = path
            .strip_prefix(&root)
            .context("attachment escaped root")?;
        let metadata = serde_json::json!({"path": relative.to_string_lossy(), "name": info.name, "mime": info.mime_type, "source": "untrusted Mattermost attachment"});
        text.push_str("\n\nAttachment metadata (untrusted data): ");
        text.push_str(&metadata.to_string());
    }
    Ok(text)
}

async fn download(client: &MattermostClient, root: &Path, id: &str, name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || name.contains(['/', '\\'])
        || name == "."
        || name == ".."
        || name.chars().any(char::is_control)
    {
        anyhow::bail!("Mattermost attachment filename is unsafe");
    }
    let data = root.join("data");
    tokio::fs::create_dir_all(&data)
        .await
        .context("attachment directory unavailable")?;
    let data = tokio::fs::canonicalize(data)
        .await
        .context("attachment directory unavailable")?;
    if !data.starts_with(root) {
        anyhow::bail!("attachment directory escaped agent root");
    }
    let uploads = data.join("uploads");
    tokio::fs::create_dir_all(&uploads)
        .await
        .context("attachment directory unavailable")?;
    let uploads = tokio::fs::canonicalize(uploads)
        .await
        .context("attachment directory unavailable")?;
    if !uploads.starts_with(root) {
        anyhow::bail!("attachment directory escaped agent root");
    }
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = uploads.join(format!(
        ".mattermost-{}-{sequence}.part",
        std::process::id()
    ));
    let target = uploads.join(format!("{}-{sequence}-{name}", std::process::id()));
    let response = client.download_file(id).await?;
    if response
        .content_length()
        .is_some_and(|bytes| bytes > MAX_ATTACHMENT_BYTES)
    {
        anyhow::bail!("Mattermost attachment exceeds 25 MiB");
    }
    // Open synchronously so cancellation cannot leave an unguarded creation task.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .context("attachment write failed")?;
    let mut pending = PartialUpload(Some(temporary.clone()));
    let mut file = tokio::fs::File::from_std(file);
    let mut stream = response.bytes_stream();
    let mut received = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow::anyhow!("attachment download failed"))?;
        received = received
            .checked_add(chunk.len() as u64)
            .context("Mattermost attachment exceeds 25 MiB")?;
        if received > MAX_ATTACHMENT_BYTES {
            anyhow::bail!("Mattermost attachment exceeds 25 MiB");
        }
        file.write_all(&chunk)
            .await
            .context("attachment write failed")?;
    }
    file.flush().await.context("attachment write failed")?;
    drop(file);
    tokio::fs::rename(&temporary, &target)
        .await
        .context("attachment rename failed")?;
    pending.0 = None;
    Ok(target)
}
