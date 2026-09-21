use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use axum::{
    body::{to_bytes, Body},
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::sync::mpsc;

const MAX_BODY: usize = 3 * 1024 * 1024;
const DEDUP_CAPACITY: usize = 5_000;
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inbound {
    pub wa_id: String,
    pub name: Option<String>,
    pub wamid: String,
    pub text: String,
}

#[derive(Default)]
pub struct Dedup {
    order: VecDeque<String>,
    ids: HashSet<String>,
}
impl Dedup {
    fn insert(&mut self, id: &str) -> bool {
        if !self.ids.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > DEDUP_CAPACITY {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
        true
    }
    fn remove(&mut self, id: &str) {
        self.ids.remove(id);
        self.order.retain(|entry| entry != id);
    }
}

#[derive(Clone)]
pub struct WebhookState {
    pub verify_token: Option<String>,
    pub app_secret: Option<String>,
    pub phone_number_id: String,
    pub inbound: mpsc::Sender<Inbound>,
    pub dedup: Arc<Mutex<Dedup>>,
}

pub fn router(state: WebhookState, path: &str) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(path, get(verify).post(inbound))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok","service":"whatsapp"}))
}
#[derive(Deserialize)]
struct VerifyQuery {
    #[serde(rename = "hub.mode")]
    mode: Option<String>,
    #[serde(rename = "hub.verify_token")]
    token: Option<String>,
    #[serde(rename = "hub.challenge")]
    challenge: Option<String>,
}
async fn verify(State(state): State<WebhookState>, Query(query): Query<VerifyQuery>) -> Response {
    let Some(expected) = state.verify_token.filter(|t| !t.is_empty()) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if query.mode.as_deref() != Some("subscribe")
        || !constant_eq(
            query.token.as_deref().unwrap_or_default().as_bytes(),
            expected.as_bytes(),
        )
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        query.challenge.unwrap_or_default(),
    )
        .into_response()
}

async fn inbound(State(state): State<WebhookState>, headers: HeaderMap, body: Body) -> Response {
    let Some(secret) = state.app_secret.filter(|s| !s.is_empty()) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let raw = match to_bytes(body, MAX_BODY).await {
        Ok(raw) => raw,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !verify_signature(&secret, &raw, signature) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let payload: Value = match serde_json::from_slice::<Value>(&raw) {
        Ok(v) if v.is_object() => v,
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    for message in parse_payload(&payload, &state.phone_number_id) {
        let fresh = state
            .dedup
            .lock()
            .expect("dedup poisoned")
            .insert(&message.wamid);
        if !fresh {
            continue;
        }
        if state.inbound.try_send(message.clone()).is_err() {
            state
                .dedup
                .lock()
                .expect("dedup poisoned")
                .remove(&message.wamid);
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    StatusCode::OK.into_response()
}

pub fn verify_signature(secret: &str, raw: &[u8], header: &str) -> bool {
    let Some(hex) = header.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(given) = hex::decode(hex) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(raw);
    mac.verify_slice(&given).is_ok()
}
fn constant_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |sum, (a, b)| sum | (a ^ b))
            == 0
}

pub fn parse_payload(payload: &Value, wanted_phone_id: &str) -> Vec<Inbound> {
    let mut result = Vec::new();
    for entry in payload
        .get("entry")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for change in entry
            .get("changes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if change.get("field").and_then(Value::as_str) != Some("messages") {
                continue;
            }
            let value = change.get("value").unwrap_or(&Value::Null);
            if value
                .pointer("/metadata/phone_number_id")
                .and_then(Value::as_str)
                != Some(wanted_phone_id)
            {
                continue;
            }
            for message in value
                .get("messages")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if message.get("type").and_then(Value::as_str) != Some("text") {
                    continue;
                }
                let Some(wa_id) = message
                    .get("from")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
                else {
                    continue;
                };
                if message.get("group_id").is_some()
                    || message
                        .get("chat_id")
                        .and_then(Value::as_str)
                        .is_some_and(|chat_id| chat_id != wa_id)
                {
                    continue;
                }
                let Some(wamid) = message
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                else {
                    continue;
                };
                let Some(text) = message
                    .pointer("/text/body")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    continue;
                };
                let name = value
                    .get("contacts")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .find(|contact| contact.get("wa_id").and_then(Value::as_str) == Some(wa_id))
                    .and_then(|contact| contact.pointer("/profile/name"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                result.push(Inbound {
                    wa_id: wa_id.to_string(),
                    name,
                    wamid: wamid.to_string(),
                    text: text.to_string(),
                });
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn new_state(capacity: usize) -> (WebhookState, mpsc::Receiver<Inbound>) {
        let (inbound, receiver) = mpsc::channel(capacity);
        (
            WebhookState {
                verify_token: Some("verify".into()),
                app_secret: Some("secret".into()),
                phone_number_id: "phone".into(),
                inbound,
                dedup: Default::default(),
            },
            receiver,
        )
    }
    fn signed_headers(body: &[u8]) -> HeaderMap {
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-hub-signature-256",
            format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
                .parse()
                .unwrap(),
        );
        headers
    }
    #[test]
    fn signs() {
        let raw = b"{}";
        let mut mac = HmacSha256::new_from_slice(b"key").unwrap();
        mac.update(raw);
        assert!(verify_signature(
            "key",
            raw,
            &format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
        ));
        assert!(!verify_signature("key", raw, "sha256=00"));
    }
    #[test]
    fn parses_text_and_name() {
        let p = json!({"entry":[{"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"p"},"contacts":[{"wa_id":"3361","profile":{"name":"A"}}],"messages":[{"from":"3361","id":"wamid","type":"text","text":{"body":"hi"}},{"from":"3361","id":"skip","type":"reaction"}]}}]}]});
        assert_eq!(
            parse_payload(&p, "p"),
            vec![Inbound {
                wa_id: "3361".into(),
                name: Some("A".into()),
                wamid: "wamid".into(),
                text: "hi".into()
            }]
        );
        assert!(parse_payload(&p, "other").is_empty());
        let group = json!({"entry":[{"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"p"},"messages":[{"from":"3361","chat_id":"group-1","id":"wamid","type":"text","text":{"body":"hi"}}]}}]}]});
        assert!(parse_payload(&group, "p").is_empty());
        let group_id = json!({"entry":[{"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"p"},"messages":[{"from":"3361","group_id":"group-1","id":"wamid","type":"text","text":{"body":"hi"}}]}}]}]});
        assert!(parse_payload(&group_id, "p").is_empty());
    }
    #[test]
    fn evicts_old_ids() {
        let mut d = Dedup::default();
        for i in 0..=DEDUP_CAPACITY {
            assert!(d.insert(&i.to_string()));
        }
        assert!(d.insert("0"));
    }
    #[tokio::test]
    async fn verifies_meta_handshake() {
        let (state, _) = new_state(1);
        let response = verify(
            State(state),
            Query(VerifyQuery {
                mode: Some("subscribe".into()),
                token: Some("verify".into()),
                challenge: Some("abc".into()),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_bytes(response.into_body(), 16).await.unwrap(), "abc");
        let (state, _) = new_state(1);
        assert_eq!(
            verify(
                State(state),
                Query(VerifyQuery {
                    mode: Some("subscribe".into()),
                    token: Some("wrong".into()),
                    challenge: None
                })
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
    #[tokio::test]
    async fn rejects_bad_requests_and_retries_queue_full() {
        let raw = br#"{"entry":[]}"#;
        let (state, _) = new_state(1);
        assert_eq!(
            inbound(State(state), HeaderMap::new(), Body::from(raw.as_slice()))
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let malformed = b"{";
        let (state, _) = new_state(1);
        assert_eq!(
            inbound(
                State(state),
                signed_headers(malformed),
                Body::from(malformed.as_slice())
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
        let payload = br#"{"entry":[{"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"phone"},"messages":[{"from":"3361","id":"id","type":"text","text":{"body":"hi"}}]}}]}]}"#;
        let (state, mut receiver) = new_state(1);
        state
            .inbound
            .try_send(Inbound {
                wa_id: "1".into(),
                name: None,
                wamid: "queued".into(),
                text: "x".into(),
            })
            .unwrap();
        assert_eq!(
            inbound(
                State(state),
                signed_headers(payload),
                Body::from(payload.as_slice())
            )
            .await
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(receiver.try_recv().is_ok());
    }
    #[tokio::test]
    async fn caps_oversized_body() {
        let body = vec![b'x'; MAX_BODY + 1];
        let (state, _) = new_state(1);
        assert_eq!(
            inbound(State(state), signed_headers(&body), Body::from(body))
                .await
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }
}
