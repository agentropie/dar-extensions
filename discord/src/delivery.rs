use anyhow::{anyhow, Result};
use serde_json::json;
use std::time::Duration;

const ATTEMPTS: usize = 3;

pub struct Delivery {
    client: reqwest::Client,
    base: String,
    token: String,
    channel: String,
    message: String,
    ack: String,
}

impl Delivery {
    pub fn new(
        client: reqwest::Client,
        token: &str,
        channel: &str,
        message: &str,
        ack: &str,
    ) -> Self {
        Self {
            client,
            base: "https://discord.com/api/v10".into(),
            token: token.into(),
            channel: channel.into(),
            message: message.into(),
            ack: ack.into(),
        }
    }

    pub async fn acknowledge(&self) -> Result<()> {
        self.reaction(&self.ack, "PUT").await
    }

    pub async fn failure(&self, _cause: &anyhow::Error) {
        let text = "Sorry, I couldn't complete that request. Please try again.";
        if self.post(&text).await.is_err() {
            let _ = self.reaction("⚠️", "PUT").await;
        }
    }

    pub async fn post(&self, content: &str) -> Result<()> {
        self.retry(|| {
            self.client
                .post(format!("{}/channels/{}/messages", self.base, self.channel))
                .header("Authorization", format!("Bot {}", self.token))
                .json(&json!({"content": content}))
        })
        .await
    }

    async fn reaction(&self, emoji: &str, method: &str) -> Result<()> {
        let url = format!(
            "{}/channels/{}/messages/{}/reactions/{}/@me",
            self.base, self.channel, self.message, emoji
        );
        self.retry(|| {
            match method {
                "PUT" => self.client.put(&url),
                _ => self.client.delete(&url),
            }
            .header("Authorization", format!("Bot {}", self.token))
        })
        .await
    }

    async fn retry<F>(&self, make: F) -> Result<()>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            match make().send().await.and_then(|r| r.error_for_status()) {
                Ok(_) => return Ok(()),
                Err(error) => {
                    last = Some(error);
                    if attempt + 1 < ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        Err(anyhow!(last.expect("failed request has an error")))
    }
}
