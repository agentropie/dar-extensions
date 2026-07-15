use crate::markdown;
use anyhow::Result;
use serde::Deserialize;
use serde_json::json;
use std::{
    future,
    time::{Duration, Instant},
};

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const SEND_ATTEMPTS: usize = 3;

pub struct LiveAnswer {
    client: reqwest::Client,
    base: String,
    token: String,
    channel: String,
    messages: Vec<String>,
    last: Option<Instant>,
    dirty: bool,
}
impl LiveAnswer {
    pub fn new(
        client: reqwest::Client,
        base: impl Into<String>,
        token: impl Into<String>,
        channel: impl Into<String>,
    ) -> Self {
        Self {
            client,
            base: base.into(),
            token: token.into(),
            channel: channel.into(),
            messages: vec![],
            last: None,
            dirty: false,
        }
    }
    pub async fn push(&mut self, answer: &str) -> Result<()> {
        if self
            .last
            .is_some_and(|last| last.elapsed() < UPDATE_INTERVAL)
        {
            self.dirty = true;
            Ok(())
        } else {
            self.flush(answer).await
        }
    }
    pub async fn finish(&mut self, answer: &str) -> Result<()> {
        if self.dirty || self.messages.is_empty() {
            self.flush(answer).await?;
        }
        Ok(())
    }
    pub async fn wait_for_flush(&self) {
        let Some(last) = self.dirty.then_some(self.last).flatten() else {
            future::pending::<()>().await;
            return;
        };
        tokio::time::sleep((last + UPDATE_INTERVAL).saturating_duration_since(Instant::now()))
            .await;
    }
    pub async fn flush_if_due(&mut self, answer: &str) -> Result<()> {
        if self.dirty {
            self.flush(answer).await?;
        }
        Ok(())
    }
    async fn flush(&mut self, answer: &str) -> Result<()> {
        for (index, content) in markdown::chunk(&markdown::render(answer))
            .iter()
            .enumerate()
        {
            let url = if let Some(id) = self.messages.get(index) {
                format!("{}/channels/{}/messages/{id}", self.base, self.channel)
            } else {
                format!("{}/channels/{}/messages", self.base, self.channel)
            };
            let request = if index < self.messages.len() {
                self.client.patch(url)
            } else {
                self.client.post(url)
            };
            let request = request
                .header("Authorization", format!("Bot {}", self.token))
                .json(&json!({"content": content}));
            let mut last_error = None;
            let mut response = None;
            for attempt in 0..SEND_ATTEMPTS {
                match request
                    .try_clone()
                    .expect("request is cloneable")
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                {
                    Ok(value) => {
                        response = Some(value);
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        if attempt + 1 < SEND_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
            let response =
                response.ok_or_else(|| last_error.expect("a failed request has an error"))?;
            if index == self.messages.len() {
                self.messages.push(response.json::<Posted>().await?.id);
            }
        }
        self.last = Some(Instant::now());
        self.dirty = false;
        Ok(())
    }
}
#[derive(Deserialize)]
struct Posted {
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    async fn request(s: &mut tokio::net::TcpStream) -> String {
        let mut b = vec![];
        loop {
            let mut x = [0; 1024];
            let n = s.read(&mut x).await.unwrap();
            b.extend_from_slice(&x[..n]);
            if b.windows(4).any(|x| x == b"\r\n\r\n") {
                return String::from_utf8(b).unwrap();
            }
        }
    }
    #[tokio::test]
    async fn coalesces_rapid_edits() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for method in ["POST", "PATCH"] {
                let (mut s, _) = listener.accept().await.unwrap();
                assert!(request(&mut s).await.starts_with(method));
                let body = r#"{"id":"1"}"#;
                s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            }
        });
        let mut answer = LiveAnswer::new(
            reqwest::Client::new(),
            format!("http://{address}"),
            "t",
            "c",
        );
        answer.push("one").await.unwrap();
        answer.push("one two").await.unwrap();
        answer.wait_for_flush().await;
        answer.flush_if_due("one two").await.unwrap();
        server.await.unwrap();
    }
}
