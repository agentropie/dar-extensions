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
    #[cfg(test)]
    pub fn with_base(base: String) -> Result<Self> {
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
            base,
            token: "test-token".into(),
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
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use std::sync::{Arc, Mutex};

    type Responses = Arc<Mutex<Vec<(StatusCode, Value)>>>;

    async fn mock_graph(
        State(responses): State<Responses>,
        Json(body): Json<Value>,
    ) -> (StatusCode, String) {
        let (status, expected) = responses.lock().unwrap().remove(0);
        assert_eq!(body, expected);
        (
            status,
            if status.is_success() {
                "ok"
            } else {
                "window closed"
            }
            .into(),
        )
    }

    async fn graph_with_responses(
        responses: Vec<(StatusCode, Value)>,
    ) -> (Graph, Responses, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let responses = Arc::new(Mutex::new(responses));
        let app = Router::new()
            .route("/messages", post(mock_graph))
            .with_state(Arc::clone(&responses));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            Graph::with_base(format!("http://{address}/messages")).unwrap(),
            responses,
            task,
        )
    }

    #[test]
    fn unicode_chunks_at_chars() {
        let s = "💬".repeat(MAX_CHARS + 1);
        let got = chunks(&s);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].chars().count(), MAX_CHARS);
    }

    #[tokio::test]
    async fn sends_recipient_and_reply_context_only_on_first_chunk() {
        let first = "x".repeat(MAX_CHARS);
        let second = "y";
        let (graph, responses, server) = graph_with_responses(vec![
            (StatusCode::OK, json!({"messaging_product":"whatsapp","recipient_type":"individual","to":"3361","type":"text","text":{"body":first,"preview_url":true},"context":{"message_id":"wamid-1"}})),
            (StatusCode::OK, json!({"messaging_product":"whatsapp","recipient_type":"individual","to":"3361","type":"text","text":{"body":second,"preview_url":true}})),
        ]).await;
        graph
            .send_text("3361", &format!("{first}{second}"), Some("wamid-1"))
            .await
            .unwrap();
        assert!(responses.lock().unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn surfaces_first_and_partial_graph_failures() {
        let payload = |body: String| json!({"messaging_product":"whatsapp","recipient_type":"individual","to":"3361","type":"text","text":{"body":body,"preview_url":true}});
        let (graph, responses, server) =
            graph_with_responses(vec![(StatusCode::BAD_REQUEST, payload("x".into()))]).await;
        let error = graph
            .send_text("3361", "x", None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("400 Bad Request"), "{error}");
        assert!(error.contains("window closed"), "{error}");
        assert!(responses.lock().unwrap().is_empty());
        server.abort();

        let first = "x".repeat(MAX_CHARS);
        let second = "y".repeat(MAX_CHARS);
        let (graph, responses, server) = graph_with_responses(vec![
            (StatusCode::OK, payload(first.clone())),
            (StatusCode::BAD_REQUEST, payload(second.clone())),
            (StatusCode::OK, payload("z".into())),
        ])
        .await;
        let error = graph
            .send_text("3361", &format!("{first}{second}z"), None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("partial delivery: 1 of 3 chunks accepted"));
        assert!(error.contains("window closed"));
        assert_eq!(responses.lock().unwrap().len(), 1);
        server.abort();
    }
}
