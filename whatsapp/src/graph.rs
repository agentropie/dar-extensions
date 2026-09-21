use anyhow::{bail, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

pub const MAX_CHARS: usize = 4096;

#[derive(Clone)]
pub struct Graph {
    client: Client,
    base: String,
    token: String,
}
impl Graph {
    pub fn new(phone_number_id: &str, token: String, api_version: &str) -> Result<Self> {
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
            base: format!("https://graph.facebook.com/{api_version}/{phone_number_id}/messages"),
            token,
        })
    }
    async fn post(&self, body: Value) -> Result<()> {
        let response = self
            .client
            .post(&self.base)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        bail!("Graph API rejected request ({status}): {text}")
    }
    pub async fn send_text(&self, to: &str, body: &str, reply_to: Option<&str>) -> Result<()> {
        let parts = chunks(body);
        let total = parts.len();
        for (i, part) in parts.into_iter().enumerate() {
            let mut payload = json!({"messaging_product":"whatsapp","recipient_type":"individual","to":to,"type":"text","text":{"body":part,"preview_url":true}});
            if i == 0 {
                if let Some(id) = reply_to {
                    payload["context"] = json!({"message_id": id});
                }
            }
            if let Err(err) = self.post(payload).await {
                if i > 0 {
                    bail!("partial delivery: {i} of {total} chunks accepted; {err:#}");
                }
                return Err(err);
            }
        }
        Ok(())
    }
    pub async fn typing(&self, wamid: &str) -> Result<()> {
        self.post(json!({"messaging_product":"whatsapp","status":"read","message_id":wamid,"typing_indicator":{"type":"text"}})).await
    }
}

pub fn chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut count = 0;
    for ch in text.chars() {
        if count == MAX_CHARS {
            out.push(std::mem::take(&mut current));
            count = 0;
        }
        current.push(ch);
        count += 1;
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_chunks_at_chars() {
        let s = "💬".repeat(MAX_CHARS + 1);
        let got = chunks(&s);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].chars().count(), MAX_CHARS);
    }
}
