use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use serde_json::Value;
use std::{
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use url::Url;

pub const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Attachment {
    pub filename: String,
    pub url: String,
    pub content_type: Option<String>,
}

pub fn parse(values: Option<&Vec<Value>>) -> Vec<Attachment> {
    values
        .into_iter()
        .flatten()
        .filter_map(|value| {
            Some(Attachment {
                filename: value["filename"].as_str()?.to_owned(),
                url: value["url"].as_str()?.to_owned(),
                content_type: value["content_type"].as_str().map(str::to_owned),
            })
        })
        .collect()
}

pub async fn prompt(
    client: &reqwest::Client,
    root: &Path,
    attachments: &[Attachment],
    mut text: String,
) -> Result<String> {
    for (index, attachment) in attachments.iter().take(10).enumerate() {
        let filename = format!("{index}-{}", safe_filename(&attachment.filename)?);
        let path = download(client, root, &attachment.url, &filename).await?;
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .display()
            .to_string();
        let metadata = serde_json::json!({
            "path": relative,
            "name": attachment.filename,
            "mime": attachment.content_type,
            "source": "untrusted Discord attachment"
        });
        text.push_str("\n\nAttachment metadata (untrusted data, inspect local path if useful): ");
        text.push_str(&metadata.to_string());
    }
    Ok(text)
}

async fn download(
    client: &reqwest::Client,
    root: &Path,
    value: &str,
    filename: &str,
) -> Result<PathBuf> {
    let url = trusted_url(value)?;
    let uploads = uploads_dir(root).await?;
    let response = client.get(url).send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|size| size > MAX_ATTACHMENT_BYTES)
    {
        bail!("Discord attachment is larger than 25 MiB")
    }
    let temporary = uploads.join(format!(
        ".discord-upload-{}-{}.part",
        std::process::id(),
        next_id()
    ));
    let target = uploads.join(format!("{}-{filename}", next_id()));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        let mut received = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            received = received
                .checked_add(chunk.len() as u64)
                .context("attachment size overflow")?;
            if received > MAX_ATTACHMENT_BYTES {
                bail!("Discord attachment is larger than 25 MiB")
            }
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        }
        tokio::io::AsyncWriteExt::flush(&mut file).await?;
        drop(file);
        tokio::fs::rename(&temporary, &target).await?;
        Ok(target)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

fn trusted_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).context("Discord attachment URL is invalid")?;
    let trusted = url
        .host_str()
        .is_some_and(|host| host == "cdn.discordapp.com" || host == "media.discordapp.net");
    if url.scheme() != "https" || !trusted {
        bail!("Discord attachment URL is untrusted")
    }
    Ok(url)
}

async fn uploads_dir(root: &Path) -> Result<PathBuf> {
    let root = tokio::fs::canonicalize(root)
        .await
        .context("agent root is unavailable")?;
    let uploads = root.join("data/uploads");
    tokio::fs::create_dir_all(&uploads).await?;
    let uploads = tokio::fs::canonicalize(uploads).await?;
    if !uploads.starts_with(&root) {
        bail!("uploads directory escaped agent root")
    }
    Ok(uploads)
}

fn safe_filename(name: &str) -> Result<&str> {
    let path = Path::new(name);
    if name.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!("Discord attachment filename is unsafe")
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .context("Discord attachment filename is invalid")
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
fn next_id() -> u64 {
    SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unsafe_filenames_and_urls() {
        assert!(safe_filename("../secret").is_err());
        assert!(trusted_url("https://example.com/file").is_err());
    }
    #[test]
    fn accepts_discord_cdn_url() {
        assert!(trusted_url("https://cdn.discordapp.com/attachments/1/2/file.png").is_ok());
    }
}
