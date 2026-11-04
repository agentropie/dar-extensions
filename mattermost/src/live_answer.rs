use crate::{api::MattermostClient, markdown};
use anyhow::Result;
use std::time::{Duration, Instant};

const UPDATE_INTERVAL: Duration = Duration::from_secs(2);
pub const ERROR_MESSAGE: &str = "Sorry, I couldn't complete that request. Please try again.";

pub async fn failure(
    client: &MattermostClient,
    bot: &str,
    source: &str,
    channel: &str,
    root: Option<&str>,
) {
    if client
        .create_post(channel, ERROR_MESSAGE, root, &[])
        .await
        .is_err()
    {
        let _ = client.add_reaction(bot, source, "warning").await;
    }
}

pub struct LiveAnswer {
    client: MattermostClient,
    channel: String,
    root: Option<String>,
    messages: Vec<(String, String)>,
    last: Option<Instant>,
    dirty: bool,
    stream: bool,
}
impl LiveAnswer {
    pub fn new(client: MattermostClient, channel: String, root: Option<String>) -> Self {
        Self {
            client,
            channel,
            root,
            messages: Vec::new(),
            last: None,
            dirty: false,
            stream: true,
        }
    }
    pub async fn push(&mut self, text: &str) -> Result<()> {
        self.dirty = true;
        if self.stream
            && self
                .last
                .is_none_or(|last| last.elapsed() >= UPDATE_INTERVAL)
        {
            self.flush(text).await?;
        }
        Ok(())
    }
    pub fn without_streaming(mut self) -> Self {
        self.stream = false;
        self
    }
    pub async fn finish(&mut self, text: &str) -> Result<()> {
        self.flush(text).await
    }
    pub async fn discard(&mut self) {
        for (id, _) in self.messages.drain(..) {
            let _ = self.client.delete_post(&id).await;
        }
        self.dirty = false;
    }
    pub async fn wait_for_flush(&self) {
        if let Some(last) = self.last.filter(|_| self.dirty && self.stream) {
            tokio::time::sleep((last + UPDATE_INTERVAL).saturating_duration_since(Instant::now()))
                .await;
        } else {
            std::future::pending::<()>().await;
        }
    }
    pub async fn flush_if_due(&mut self, text: &str) -> Result<()> {
        if self.dirty
            && self.stream
            && self
                .last
                .is_some_and(|last| last.elapsed() >= UPDATE_INTERVAL)
        {
            self.flush(text).await?;
        }
        Ok(())
    }
    async fn flush(&mut self, text: &str) -> Result<()> {
        let rendered = markdown::render(text);
        let chunks = markdown::chunk(&rendered);
        for (index, chunk) in chunks.iter().enumerate() {
            if let Some((id, previous)) = self.messages.get_mut(index) {
                if previous != chunk {
                    self.client.patch_post(id, chunk).await?;
                    *previous = chunk.clone();
                }
            } else {
                let post = self
                    .client
                    .create_post(&self.channel, chunk, self.root.as_deref(), &[])
                    .await?;
                self.messages.push((post.id, chunk.clone()));
            }
        }
        while self.messages.len() > chunks.len() {
            let (id, _) = self.messages.last().expect("excess post");
            self.client.delete_post(id).await?;
            self.messages.pop();
        }
        self.last = Some(Instant::now());
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::SecretToken,
        test_http::{Response, Server},
    };
    use serde_json::{json, Value};

    #[tokio::test]
    async fn first_delta_creates_a_reply_in_the_requested_thread() {
        let mut server = Server::start(vec![Response::json(
            json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"}),
        )])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(
            client,
            "cccccccccccccccccccccccccc".into(),
            Some("tttttttttttttttttttttttttt".into()),
        );
        live.push("Hello").await.unwrap();
        let request = server.next().await;
        assert_eq!(
            (request.method.as_str(), request.path.as_str()),
            ("POST", "/api/v4/posts")
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["message"], "Hello");
        assert_eq!(body["root_id"], "tttttttttttttttttttttttttt");
        server.finish().await;
    }

    #[tokio::test]
    async fn rapid_deltas_coalesce_into_one_patch_on_finish() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(client, "cccccccccccccccccccccccccc".into(), None);
        live.push("one").await.unwrap();
        live.push("one two").await.unwrap();
        live.finish("one two").await.unwrap();
        assert_eq!(server.next().await.method, "POST");
        let update = server.next().await;
        assert_eq!(
            (update.method.as_str(), update.path.as_str()),
            ("PUT", "/api/v4/posts/rrrrrrrrrrrrrrrrrrrrrrrrrr/patch")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&update.body).unwrap()["message"],
            "one two"
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn bot_reply_posts_once_complete_without_edits() {
        let mut server = Server::start(vec![Response::json(
            json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"}),
        )])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live =
            LiveAnswer::new(client, "cccccccccccccccccccccccccc".into(), None).without_streaming();
        live.push("one").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), server.next())
                .await
                .is_err(),
            "bot reply must wait until complete"
        );
        live.push("one two").await.unwrap();
        live.finish("one two").await.unwrap();
        let request = server.next().await;
        assert_eq!(request.method, "POST");
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["message"],
            "one two"
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn late_silence_deletes_an_already_streamed_reply() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
            Response::json(json!({})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(client, "cccccccccccccccccccccccccc".into(), None);
        live.push("partial").await.unwrap();
        live.discard().await;
        server.next().await;
        let deleted = server.next().await;
        assert_eq!(
            (deleted.method.as_str(), deleted.path.as_str()),
            ("DELETE", "/api/v4/posts/rrrrrrrrrrrrrrrrrrrrrrrrrr")
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn failed_error_delivery_retries_three_times_then_marks_source_warning() {
        let mut server = Server::start(vec![
            Response::status(503),
            Response::status(503),
            Response::status(503),
            Response::json(json!({})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        failure(
            &client,
            "bbbbbbbbbbbbbbbbbbbbbbbbbb",
            "pppppppppppppppppppppppppp",
            "cccccccccccccccccccccccccc",
            Some("tttttttttttttttttttttttttt"),
        )
        .await;
        for _ in 0..3 {
            let request = server.next().await;
            assert_eq!(request.path, "/api/v4/posts");
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap()["message"],
                ERROR_MESSAGE
            );
        }
        let warning = server.next().await;
        assert_eq!(warning.path, "/api/v4/reactions");
        assert_eq!(
            serde_json::from_slice::<Value>(&warning.body).unwrap(),
            json!({"user_id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","post_id":"pppppppppppppppppppppppppp","emoji_name":"warning"})
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn long_replies_split_unicode_and_balance_fences_without_broadcast_pings() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa"})),
            Response::json(json!({"id":"eeeeeeeeeeeeeeeeeeeeeeeeee"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(
            client,
            "cccccccccccccccccccccccccc".into(),
            Some("tttttttttttttttttttttttttt".into()),
        )
        .without_streaming();
        live.finish(&format!("@channel\n```rust\n{}\n```", "界\n".repeat(8_000)))
            .await
            .unwrap();
        let mut messages = Vec::new();
        for _ in 0..2 {
            let request = server.next().await;
            assert_eq!(request.method, "POST");
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["root_id"], "tttttttttttttttttttttttttt");
            let text = body["message"].as_str().unwrap().to_owned();
            assert!(text.chars().count() <= 15_000);
            assert_eq!(text.matches("```").count(), 2);
            messages.push(text);
        }
        assert!(messages[0].starts_with("@\u{200b}channel\n```rust\n"));
        assert!(messages[1].starts_with("```rust\n"));
        assert_eq!(
            messages.concat().chars().filter(|c| *c == '界').count(),
            8_000
        );
        server.finish().await;
    }

    #[tokio::test]
    async fn dirty_reply_waits_two_seconds_then_flushes_without_another_delta() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(client, "cccccccccccccccccccccccccc".into(), None);
        live.push("one").await.unwrap();
        server.next().await;
        live.push("one two").await.unwrap();
        live.flush_if_due("one two").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), server.next())
                .await
                .is_err(),
            "early flush must coalesce"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(40), live.wait_for_flush())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(3), live.wait_for_flush())
            .await
            .unwrap();
        live.flush_if_due("one two").await.unwrap();
        let request = server.next().await;
        assert_eq!(request.method, "PUT");
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["message"],
            "one two"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(40), live.wait_for_flush())
                .await
                .is_err()
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn finish_replaces_the_last_streamed_draft_with_the_final_reply() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
            Response::json(json!({"id":"rrrrrrrrrrrrrrrrrrrrrrrrrr"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let mut live = LiveAnswer::new(client, "cccccccccccccccccccccccccc".into(), None);
        live.push("Draft").await.unwrap();
        server.next().await;
        live.finish("Final").await.unwrap();
        let request = tokio::time::timeout(Duration::from_millis(200), server.next())
            .await
            .expect("final reply should be visible");
        assert_eq!(request.method, "PUT");
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["message"],
            "Final"
        );
        server.finish().await;
    }
}
