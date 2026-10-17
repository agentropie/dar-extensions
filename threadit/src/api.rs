//! Threadit HTTP client and the JSON shapes the extension reads.

use std::fmt;
use std::time::Duration;

use reqwest::{header, Method};
use serde::Deserialize;
use serde_json::{json, Value};

/// Retries for GETs and reactions after the first attempt.
const RETRIES: u32 = 3;
/// Longest message body the board accepts.
pub const MAX_BODY_CHARS: usize = 19_000;

/// A board error. The text never holds the token or raw server text.
#[derive(Debug)]
pub enum BoardError {
    ThreadClosed,
    Status { status: u16, code: String },
    Network(&'static str),
    Decode,
}

impl fmt::Display for BoardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoardError::ThreadClosed => write!(f, "thread is closed"),
            BoardError::Status { status, code } => write!(f, "board returned {status} {code}"),
            BoardError::Network(kind) => write!(f, "network error: {kind}"),
            BoardError::Decode => write!(f, "unexpected board response"),
        }
    }
}

impl std::error::Error for BoardError {}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub account_type: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Attachment {
    #[serde(default)]
    pub filename: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Session {
    #[serde(default)]
    pub agent: Account,
    #[serde(default)]
    pub stop_requested: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Message {
    pub id: i64,
    #[serde(default)]
    pub thread_id: i64,
    #[serde(default)]
    pub author: Account,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub agent_sessions: Vec<Session>,
}

impl Message {
    /// True when the session of `agent_id` on this message has a stop request.
    pub fn stop_requested_for(&self, agent_id: i64) -> bool {
        self.agent_sessions
            .iter()
            .any(|s| s.agent.id == agent_id && s.stop_requested.is_some())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Area {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ThreadInfo {
    #[serde(default)]
    pub title: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Context {
    #[serde(default)]
    pub area: Area,
    #[serde(default)]
    pub thread: ThreadInfo,
    pub target_id: i64,
    #[serde(default)]
    pub top_level: Vec<Message>,
    #[serde(default)]
    pub chain: Vec<Message>,
}

#[derive(Deserialize)]
struct AttentionPage {
    #[serde(default)]
    items: Vec<Message>,
    #[serde(default)]
    has_more: bool,
}

#[derive(Clone)]
pub struct Board {
    http: reqwest::Client,
    base: String,
    retry_delay: Duration,
}

impl Board {
    pub fn new(base_url: &str, token: &str) -> anyhow::Result<Self> {
        let mut auth = header::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            anyhow::anyhow!("THREADIT_TOKEN holds characters not valid in a header")
        })?;
        auth.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth);
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| anyhow::anyhow!("could not build the HTTP client"))?;
        Ok(Self {
            http,
            base: base_url.trim_end_matches('/').to_string(),
            retry_delay: Duration::from_millis(500),
        })
    }

    #[cfg(test)]
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Base URL, used to tell the agent where the API guide lives.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        retry: bool,
    ) -> Result<Value, BoardError> {
        let url = format!("{}{}", self.base, path);
        let attempts = if retry { RETRIES + 1 } else { 1 };
        let mut last = BoardError::Network("request failed");
        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(self.retry_delay * attempt).await;
            }
            let mut req = self.http.request(method.clone(), &url);
            if let Some(body) = body {
                req = req.json(body);
            }
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(err) => {
                    last = BoardError::Network(if err.is_timeout() {
                        "timeout"
                    } else if err.is_connect() {
                        "connect"
                    } else {
                        "request failed"
                    });
                    continue;
                }
            };
            let status = resp.status();
            let text = match resp.text().await {
                Ok(text) => text,
                Err(_) => {
                    last = BoardError::Network("read failed");
                    continue;
                }
            };
            if status.is_success() {
                return if text.trim().is_empty() {
                    Ok(Value::Null)
                } else {
                    serde_json::from_str(&text).map_err(|_| BoardError::Decode)
                };
            }
            let code = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
                .unwrap_or_default();
            if status.as_u16() == 409 && code == "thread_closed" {
                return Err(BoardError::ThreadClosed);
            }
            last = BoardError::Status {
                status: status.as_u16(),
                code,
            };
            if !status.is_server_error() {
                break;
            }
        }
        Err(last)
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, BoardError> {
        let value = self.request(Method::GET, path, None, true).await?;
        serde_json::from_value(value).map_err(|_| BoardError::Decode)
    }

    pub async fn me(&self) -> Result<Account, BoardError> {
        self.get("/api/me").await
    }

    /// One page of attention items.
    pub async fn attention(
        &self,
        status: &str,
        offset: usize,
    ) -> Result<(Vec<Message>, bool), BoardError> {
        let page: AttentionPage = self
            .get(&format!(
                "/api/attention?status={status}&order=asc&limit=50&offset={offset}"
            ))
            .await?;
        Ok((page.items, page.has_more))
    }

    pub async fn context(&self, message_id: i64) -> Result<Context, BoardError> {
        self.get(&format!("/api/messages/{message_id}/context"))
            .await
    }

    pub async fn message(&self, message_id: i64) -> Result<Message, BoardError> {
        self.get(&format!("/api/messages/{message_id}")).await
    }

    /// Add a reaction. Reactions are idempotent, so this retries.
    pub async fn react(&self, message_id: i64, emoji: &str) -> Result<(), BoardError> {
        self.request(
            Method::POST,
            &format!("/api/messages/{message_id}/reactions"),
            Some(&json!({ "emoji": emoji })),
            true,
        )
        .await
        .map(|_| ())
    }

    /// Post an activity. Returns the session of the response (it carries `stop_requested`).
    /// Never retried.
    pub async fn activity(
        &self,
        message_id: i64,
        kind: &str,
        body: &str,
        ephemeral: bool,
    ) -> Result<Session, BoardError> {
        let mut payload = json!({ "kind": kind, "body": truncate_chars(body, 2000) });
        if ephemeral {
            payload["ephemeral"] = json!(true);
        }
        let value = self
            .request(
                Method::POST,
                &format!("/api/messages/{message_id}/agent-activities"),
                Some(&payload),
                false,
            )
            .await?;
        serde_json::from_value(value).map_err(|_| BoardError::Decode)
    }

    /// Post a reply. Never retried: a lost response must not post twice.
    pub async fn reply(
        &self,
        thread_id: i64,
        parent_id: i64,
        body: &str,
    ) -> Result<(), BoardError> {
        self.request(
            Method::POST,
            &format!("/api/threads/{thread_id}/messages"),
            Some(&json!({ "body": body, "parent_id": parent_id })),
            false,
        )
        .await
        .map(|_| ())
    }
}

pub fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Split an answer into parts of at most `max` chars, preferring line breaks.
pub fn split_answer(text: &str, max: usize) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = text.trim();
    while rest.chars().count() > max {
        let cut = rest
            .char_indices()
            .nth(max)
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        let split_at = rest[..cut]
            .rfind('\n')
            .filter(|i| *i > cut / 2)
            .unwrap_or(cut);
        let (head, tail) = rest.split_at(split_at);
        parts.push(head.trim_end().to_string());
        rest = tail.trim_start_matches('\n');
    }
    if !rest.is_empty() {
        parts.push(rest.to_string());
    }
    parts
}

#[cfg(test)]
pub(crate) mod mock {
    //! A hand-written HTTP server that records requests and answers from a closure.

    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[derive(Clone, Debug)]
    pub struct Req {
        pub method: String,
        pub path: String,
        pub body: String,
        pub auth: String,
    }

    pub type Handler = Arc<dyn Fn(&Req, usize) -> (u16, String) + Send + Sync>;

    pub struct MockServer {
        pub base: String,
        pub requests: Arc<Mutex<Vec<Req>>>,
    }

    impl MockServer {
        pub async fn start(handler: Handler) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let requests: Arc<Mutex<Vec<Req>>> = Arc::default();
            let log = requests.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        return;
                    };
                    let log = log.clone();
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let Some(req) = read_request(&mut sock).await else {
                            return;
                        };
                        let n = {
                            let mut guard = log.lock().unwrap();
                            guard.push(req.clone());
                            guard.len()
                        };
                        let (status, body) = handler(&req, n);
                        let reason = if status < 300 { "OK" } else { "ERR" };
                        let resp = format!(
                            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    });
                }
            });
            Self { base, requests }
        }

        pub fn requests(&self) -> Vec<Req> {
            self.requests.lock().unwrap().clone()
        }
    }

    async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<Req> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let mut lines = head.lines();
        let mut first = lines.next()?.split_whitespace();
        let method = first.next()?.to_string();
        let path = first.next()?.to_string();
        let mut len = 0usize;
        let mut auth = String::new();
        for line in lines {
            let lower = line.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                len = v.trim().parse().unwrap_or(0);
            }
            if lower.starts_with("authorization:") {
                auth = line.split_once(':')?.1.trim().to_string();
            }
        }
        while buf.len() < header_end + len {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = String::from_utf8_lossy(&buf[header_end..]).to_string();
        Some(Req {
            method,
            path,
            body,
            auth,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::mock::*;
    use super::*;

    fn board(base: &str) -> Board {
        Board::new(base, "tok-123")
            .unwrap()
            .with_retry_delay(Duration::from_millis(1))
    }

    #[tokio::test]
    async fn get_retries_5xx_and_sends_bearer() {
        let server = MockServer::start(Arc::new(|_, n| {
            if n < 3 {
                (500, "{}".into())
            } else {
                (
                    200,
                    r#"{"id":5,"username":"bot","account_type":"agent"}"#.into(),
                )
            }
        }))
        .await;
        let me = board(&server.base).me().await.unwrap();
        assert_eq!(me.username, "bot");
        let reqs = server.requests();
        assert_eq!(reqs.len(), 3);
        assert_eq!(reqs[0].auth, "Bearer tok-123");
    }

    #[tokio::test]
    async fn message_post_is_not_retried_and_error_hides_secrets() {
        let server = MockServer::start(Arc::new(|_, _| {
            (
                500,
                r#"{"error":{"code":"boom","message":"/secret/path tok-123"}}"#.into(),
            )
        }))
        .await;
        let err = board(&server.base).reply(1, 2, "hi").await.unwrap_err();
        assert_eq!(server.requests().len(), 1);
        let text = err.to_string();
        assert!(!text.contains("tok-123") && !text.contains("/secret"));
    }

    #[tokio::test]
    async fn thread_closed_is_detected() {
        let server = MockServer::start(Arc::new(|_, _| {
            (
                409,
                r#"{"error":{"code":"thread_closed","message":"x"}}"#.into(),
            )
        }))
        .await;
        let err = board(&server.base)
            .activity(1, "thought", "Started", false)
            .await
            .unwrap_err();
        assert!(matches!(err, BoardError::ThreadClosed));
    }

    #[test]
    fn split_answer_respects_limit() {
        let text = "a".repeat(45_000);
        let parts = split_answer(&text, MAX_BODY_CHARS);
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|p| p.chars().count() <= MAX_BODY_CHARS));
        assert_eq!(parts.concat(), text);
        assert_eq!(split_answer("short", 10), vec!["short"]);
    }
}
