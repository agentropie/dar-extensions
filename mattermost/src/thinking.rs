use crate::{api::MattermostClient, markdown};
use anyhow::Result;
use std::time::{Duration, Instant};
const MAX_THINKING_CHARS: usize = 3_000;
const UPDATE_INTERVAL: Duration = Duration::from_secs(3);
pub struct Thinking {
    client: MattermostClient,
    channel: String,
    root: Option<String>,
    delete_on_complete: bool,
    text: String,
    post: Option<String>,
    last: Option<Instant>,
    dirty: bool,
}
impl Thinking {
    pub fn new(
        client: MattermostClient,
        channel: String,
        root: Option<String>,
        delete_on_complete: bool,
    ) -> Self {
        Self {
            client,
            channel,
            root,
            delete_on_complete,
            text: String::new(),
            post: None,
            last: None,
            dirty: false,
        }
    }
    pub async fn append(&mut self, text: &str) -> Result<()> {
        let before = self.text.len();
        self.text.extend(
            text.chars()
                .take(MAX_THINKING_CHARS.saturating_sub(self.text.chars().count())),
        );
        if self.text.len() == before {
            return Ok(());
        }
        self.dirty = true;
        if self
            .last
            .is_none_or(|last| last.elapsed() >= UPDATE_INTERVAL)
        {
            self.flush().await?;
        }
        Ok(())
    }
    pub async fn wait_for_flush(&self) {
        if let Some(last) = self.last.filter(|_| self.dirty) {
            tokio::time::sleep((last + UPDATE_INTERVAL).saturating_duration_since(Instant::now()))
                .await;
        } else {
            std::future::pending::<()>().await;
        }
    }
    pub async fn flush_if_due(&mut self) -> Result<()> {
        if self.dirty
            && self
                .last
                .is_some_and(|last| last.elapsed() >= UPDATE_INTERVAL)
        {
            self.flush().await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        let display: String = markdown::render(&format!("🧠 Thinking:\n{}", self.text))
            .chars()
            .take(MAX_THINKING_CHARS)
            .collect();
        if let Some(id) = &self.post {
            self.client.patch_post(id, &display).await?;
        } else {
            self.post = Some(
                self.client
                    .create_post(&self.channel, &display, self.root.as_deref(), &[])
                    .await?
                    .id,
            );
        }
        self.last = Some(Instant::now());
        self.dirty = false;
        Ok(())
    }
    pub async fn finish(&mut self, force_delete: bool) -> Result<()> {
        if force_delete || self.delete_on_complete {
            if let Some(id) = &self.post {
                self.client.delete_post(id).await?;
                self.post = None;
            }
            self.dirty = false;
        } else if self.dirty {
            self.flush().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::MattermostClient,
        config::SecretToken,
        test_http::{Response, Server},
    };
    use serde_json::{json, Value};
    use std::time::Duration;
    #[tokio::test]
    async fn thinking_is_capped_in_its_thread_and_silence_forces_deletion_even_when_retained() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
            Response::json(json!({})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut thinking = Thinking::new(
            client,
            "cccccccccccccccccccccccccc".into(),
            Some("tttttttttttttttttttttttttt".into()),
            false,
        );
        thinking
            .append(&format!("@channel {}", "界".repeat(3_100)))
            .await
            .unwrap();
        let created = server.next().await;
        assert_eq!(created.method, "POST");
        let body: Value = serde_json::from_slice(&created.body).unwrap();
        assert_eq!(body["root_id"], "tttttttttttttttttttttttttt");
        let text = body["message"].as_str().unwrap();
        assert!(text.starts_with("🧠 Thinking:\n@\u{200b}channel "));
        assert_eq!(text.chars().count(), 3_000);
        thinking.finish(false).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), server.next())
                .await
                .is_err()
        );
        thinking.finish(true).await.unwrap();
        let deleted = server.next().await;
        assert_eq!(
            (deleted.method.as_str(), deleted.path.as_str()),
            ("DELETE", "/api/v4/posts/hhhhhhhhhhhhhhhhhhhhhhhhhh")
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn thinking_coalesces_for_three_seconds_then_flushes_and_default_cleanup_deletes() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
            Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
            Response::json(json!({})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut thinking = Thinking::new(client, "cccccccccccccccccccccccccc".into(), None, true);
        thinking.append("first").await.unwrap();
        server.next().await;
        thinking.append(" second").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), server.next())
                .await
                .is_err(),
            "thinking should coalesce"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(40), thinking.wait_for_flush())
                .await
                .is_err()
        );
        thinking.flush_if_due().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), server.next())
                .await
                .is_err(),
            "early flush should coalesce"
        );
        tokio::time::timeout(Duration::from_secs(4), thinking.wait_for_flush())
            .await
            .unwrap();
        thinking.flush_if_due().await.unwrap();
        let updated = server.next().await;
        assert_eq!(updated.method, "PUT");
        assert_eq!(
            serde_json::from_slice::<Value>(&updated.body).unwrap()["message"],
            "🧠 Thinking:\nfirst second"
        );
        thinking.finish(false).await.unwrap();
        assert_eq!(server.next().await.method, "DELETE");
        server.finish().await;
    }
}
