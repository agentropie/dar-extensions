use std::{
    future,
    time::{Duration, Instant},
};

use crate::{
    api::{SentMessage, SlackClient},
    mrkdwn,
};

const MAX_MESSAGE_BYTES: usize = 3900;
const UPDATE_INTERVAL: Duration = Duration::from_secs(3);

/// The in-progress assistant reply. Unlike thinking, this stays visible when
/// the turn ends; later deltas edit its existing Slack messages.
pub struct LiveAnswer {
    client: SlackClient,
    channel: String,
    thread_ts: Option<String>,
    messages: Vec<SentMessage>,
    failed: bool,
    last_update: Option<Instant>,
    dirty: bool,
}

impl LiveAnswer {
    pub fn new(client: SlackClient, channel: String, thread_ts: Option<String>) -> Self {
        Self {
            client,
            channel,
            thread_ts,
            messages: Vec::new(),
            failed: false,
            last_update: None,
            dirty: false,
        }
    }

    pub async fn push(&mut self, answer: &str) {
        if !self.messages.is_empty()
            && self
                .last_update
                .is_some_and(|last_update| last_update.elapsed() < UPDATE_INTERVAL)
        {
            self.dirty = true;
            return;
        }
        self.flush(answer).await;
    }

    /// Wait until a coalesced update is due. With no pending update this never
    /// completes, so callers can use it directly as a `select!` branch.
    pub async fn wait_for_flush(&self) {
        let Some(last_update) = self.dirty.then_some(self.last_update).flatten() else {
            future::pending::<()>().await;
            return;
        };
        tokio::time::sleep(
            (last_update + UPDATE_INTERVAL).saturating_duration_since(Instant::now()),
        )
        .await;
    }

    pub async fn flush_if_due(&mut self, answer: &str) {
        if self.dirty {
            self.flush(answer).await;
        }
    }

    async fn flush(&mut self, answer: &str) {
        let chunks = mrkdwn::chunk(&mrkdwn::render(answer), MAX_MESSAGE_BYTES);
        for (index, chunk) in chunks.iter().enumerate() {
            if let Some(message) = self.messages.get(index) {
                if self
                    .client
                    .update_message(&self.channel, &message.ts, chunk)
                    .await
                    .is_err()
                {
                    self.failed = true;
                    self.dirty = false;
                    return;
                }
            } else {
                match self
                    .client
                    .post_message(&self.channel, chunk, self.thread_ts.as_deref())
                    .await
                {
                    Ok(message) => self.messages.push(message),
                    Err(_) => {
                        // Do not post a later chunk before this one. Retrying
                        // on the next flush preserves the chunk/message map.
                        self.failed = true;
                        self.dirty = false;
                        return;
                    }
                }
            }
        }
        self.last_update = Some(Instant::now());
        self.dirty = false;
    }

    pub async fn finish(mut self, answer: &str) -> (bool, bool) {
        if self.dirty || (self.failed && !self.messages.is_empty()) {
            self.flush(answer).await;
        }
        (!self.messages.is_empty(), !self.failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            bytes.extend_from_slice(&buffer[..read]);
            let Some(headers_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if bytes.len() >= headers_end + 4 + length {
                return String::from_utf8(bytes).unwrap();
            }
        }
    }

    #[test]
    fn rendered_answer_stays_within_slack_limit() {
        let answer = "é".repeat(3000);
        let chunks = mrkdwn::chunk(&mrkdwn::render(&answer), MAX_MESSAGE_BYTES);
        assert!(chunks.iter().all(|chunk| chunk.len() <= MAX_MESSAGE_BYTES));
        assert_eq!(chunks.concat(), answer);
    }

    #[tokio::test]
    async fn posts_first_delta_then_flushes_coalesced_update_before_finish() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (path, required) in [
                ("/chat.postMessage", "\"thread_ts\":\"1.0\""),
                ("/chat.update", "\"ts\":\"2.0\""),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                assert!(request.starts_with(&format!("POST {path}")));
                assert!(request.contains(required));
                let body = r#"{"ok":true,"channel":"C1","ts":"2.0"}"#;
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let client = SlackClient::with_base("token".into(), &format!("http://{address}/")).unwrap();
        let mut answer = LiveAnswer::new(client, "C1".into(), Some("1.0".into()));
        answer.push("first").await;
        answer.push("first second").await;
        assert_eq!(answer.messages.len(), 1);
        tokio::time::pause();
        {
            let wait = answer.wait_for_flush();
            tokio::pin!(wait);
            tokio::select! {
                _ = &mut wait => panic!("coalesced update flushed too early"),
                _ = tokio::time::sleep(Duration::ZERO) => {}
            }
            tokio::time::advance(UPDATE_INTERVAL).await;
            wait.await;
        }
        tokio::time::resume();
        answer.flush_if_due("first second").await;
        let (displayed, succeeded) = answer.finish("first second").await;
        assert!(displayed && succeeded);
        server.await.unwrap();
    }
}
