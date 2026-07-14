use std::time::Duration;

use reqwest::{header, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use url::Url;

const API_BASE: &str = "https://slack.com/api/";
const MAX_LIST_PAGES: usize = 10;

#[derive(Clone)]
pub struct SlackClient {
    client: reqwest::Client,
    base: Url,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackError {
    pub code: String,
    pub retry_after: Option<Duration>,
}

impl std::fmt::Display for SlackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Slack API error: {}", self.code)
    }
}

impl std::error::Error for SlackError {}

#[derive(Debug, Deserialize)]
struct SlackEnvelope<T> {
    #[serde(flatten)]
    value: T,
}

#[derive(Debug, Deserialize)]
struct SlackStatus {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct Empty {}

#[derive(Debug, Deserialize)]
struct ConnectionResponse {
    url: String,
}

#[derive(Debug, Deserialize)]
struct AuthResponse {
    user_id: String,
    team_id: String,
    #[serde(default)]
    bot_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageResponse {
    channel: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct RepliesResponse {
    #[serde(default)]
    messages: Vec<ReplyMessage>,
}

#[derive(Debug, Deserialize)]
struct ReplyMessage {
    ts: String,
    #[serde(default)]
    thread_ts: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConversationOpenResponse {
    channel: OpenedChannel,
}

#[derive(Debug, Deserialize)]
struct OpenedChannel {
    id: String,
}

#[derive(Debug, Deserialize)]
struct UsersPage {
    members: Vec<SlackUser>,
    #[serde(default)]
    response_metadata: ResponseMetadata,
}

#[derive(Debug, Deserialize)]
struct ChannelsPage {
    channels: Vec<SlackChannel>,
    #[serde(default)]
    response_metadata: ResponseMetadata,
}

#[derive(Debug, Deserialize, Default)]
struct ResponseMetadata {
    #[serde(default)]
    next_cursor: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SlackIdentity {
    pub user_id: String,
    pub team_id: String,
    pub bot_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SentMessage {
    pub channel: String,
    pub ts: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SlackUser {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub real_name: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SlackChannel {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub is_archived: bool,
    #[serde(default)]
    pub is_member: bool,
}

impl SlackClient {
    pub fn new(token: String) -> Result<Self, SlackError> {
        Self::with_base(token, API_BASE)
    }

    pub fn with_base(token: String, base: &str) -> Result<Self, SlackError> {
        let mut headers = header::HeaderMap::new();
        let value = header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| SlackError::new("invalid_auth_header", None))?;
        headers.insert(header::AUTHORIZATION, value);
        let base = Url::parse(base).map_err(|_| SlackError::new("invalid_api_base", None))?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SlackError::new("client_init_failed", None))?;
        Ok(Self { client, base })
    }

    pub async fn socket_mode_url(&self, app_token: &str) -> Result<String, SlackError> {
        let response = self
            .client
            .post(self.endpoint("apps.connections.open")?)
            .bearer_auth(app_token)
            .send()
            .await
            .map_err(|_| SlackError::new("socket_open_request_failed", None))?;
        let response: ConnectionResponse = self.decode(response).await?;
        if response.url.starts_with("wss://") {
            Ok(response.url)
        } else {
            Err(SlackError::new("invalid_socket_url", None))
        }
    }

    pub async fn auth_test(&self) -> Result<SlackIdentity, SlackError> {
        let response = self
            .client
            .post(self.endpoint("auth.test")?)
            .send()
            .await
            .map_err(|_| SlackError::new("auth_test_failed", None))?;
        let value: AuthResponse = self.decode(response).await?;
        Ok(SlackIdentity {
            user_id: value.user_id,
            team_id: value.team_id,
            bot_id: value.bot_id,
        })
    }

    pub async fn open_direct_message(&self, user: &str) -> Result<String, SlackError> {
        let response = self
            .client
            .post(self.endpoint("conversations.open")?)
            .json(&serde_json::json!({"users": user}))
            .send()
            .await
            .map_err(|_| SlackError::new("open_conversation_failed", None))?;
        let value: ConversationOpenResponse = self.decode(response).await?;
        Ok(value.channel.id)
    }

    pub async fn post_message(
        &self,
        channel: &str,
        text: &str,
        thread_ts: Option<&str>,
    ) -> Result<SentMessage, SlackError> {
        let mut body = serde_json::json!({"channel": channel, "text": text, "mrkdwn": true, "unfurl_links": false, "unfurl_media": false});
        if let Some(thread_ts) = thread_ts {
            body["thread_ts"] = serde_json::Value::String(thread_ts.into());
        }
        let response = self
            .client
            .post(self.endpoint("chat.postMessage")?)
            .json(&body)
            .send()
            .await
            .map_err(|_| SlackError::new("post_message_failed", None))?;
        let value: MessageResponse = self.decode(response).await?;
        Ok(SentMessage {
            channel: value.channel,
            ts: value.ts,
        })
    }

    pub async fn update_message(
        &self,
        channel: &str,
        timestamp: &str,
        text: &str,
    ) -> Result<(), SlackError> {
        let response = self
            .client
            .post(self.endpoint("chat.update")?)
            .json(&serde_json::json!({"channel": channel, "ts": timestamp, "text": text, "unfurl_links": false, "unfurl_media": false}))
            .send()
            .await
            .map_err(|_| SlackError::new("update_message_failed", None))?;
        let _: MessageResponse = self.decode(response).await?;
        Ok(())
    }

    pub async fn delete_message(&self, channel: &str, timestamp: &str) -> Result<(), SlackError> {
        let response = self
            .client
            .post(self.endpoint("chat.delete")?)
            .json(&serde_json::json!({"channel": channel, "ts": timestamp}))
            .send()
            .await
            .map_err(|_| SlackError::new("delete_message_failed", None))?;
        let _: serde_json::Value = self.decode(response).await?;
        Ok(())
    }

    pub async fn message_thread_ts(
        &self,
        channel: &str,
        timestamp: &str,
    ) -> Result<Option<String>, SlackError> {
        let response = self
            .client
            .get(self.endpoint("conversations.replies")?)
            .query(&[("channel", channel), ("ts", timestamp)])
            .send()
            .await
            .map_err(|_| SlackError::new("conversation_replies_failed", None))?;
        let replies: RepliesResponse = self.decode(response).await?;
        Ok(replies
            .messages
            .into_iter()
            .find(|message| message.ts == timestamp)
            .and_then(|message| message.thread_ts))
    }

    pub async fn add_reaction(
        &self,
        channel: &str,
        timestamp: &str,
        name: &str,
    ) -> Result<(), SlackError> {
        self.reaction("reactions.add", channel, timestamp, name)
            .await
    }

    pub async fn remove_reaction(
        &self,
        channel: &str,
        timestamp: &str,
        name: &str,
    ) -> Result<(), SlackError> {
        self.reaction("reactions.remove", channel, timestamp, name)
            .await
    }

    async fn reaction(
        &self,
        method: &str,
        channel: &str,
        timestamp: &str,
        name: &str,
    ) -> Result<(), SlackError> {
        let response = self
            .client
            .post(self.endpoint(method)?)
            .json(&serde_json::json!({"channel":channel,"timestamp":timestamp,"name":name}))
            .send()
            .await
            .map_err(|_| SlackError::new("reaction_request_failed", None))?;
        self.decode::<Empty>(response).await.map(|_| ())
    }

    pub async fn list_users(
        &self,
        limit: usize,
        query: Option<&str>,
    ) -> Result<Vec<SlackUser>, SlackError> {
        let mut users = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_LIST_PAGES {
            if users.len() >= limit {
                break;
            }
            let response = self.list_request("users.list", &cursor).await?;
            let page: UsersPage = self.decode(response).await?;
            let cursor_next = page.response_metadata.next_cursor;
            let query = query.map(str::to_lowercase);
            let filtered = public_users(page.members)
                .into_iter()
                .filter(|user| matches_query(query.as_deref(), &[&user.name, &user.real_name]));
            extend_page(&mut users, filtered.collect(), limit);
            if cursor_next.is_empty() {
                break;
            }
            cursor = cursor_next;
        }
        Ok(users)
    }

    pub async fn list_channels(
        &self,
        limit: usize,
        query: Option<&str>,
    ) -> Result<Vec<SlackChannel>, SlackError> {
        let mut channels = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_LIST_PAGES {
            if channels.len() >= limit {
                break;
            }
            let response = self.list_request("conversations.list", &cursor).await?;
            let page: ChannelsPage = self.decode(response).await?;
            let cursor_next = page.response_metadata.next_cursor;
            let query = query.map(str::to_lowercase);
            let filtered = public_channels(page.channels)
                .into_iter()
                .filter(|channel| matches_query(query.as_deref(), &[&channel.name]));
            extend_page(&mut channels, filtered.collect(), limit);
            if cursor_next.is_empty() {
                break;
            }
            cursor = cursor_next;
        }
        Ok(channels)
    }

    async fn list_request(
        &self,
        method: &str,
        cursor: &str,
    ) -> Result<reqwest::Response, SlackError> {
        self.client
            .get(self.endpoint(method)?)
            .query(&[("limit", "200"), ("cursor", cursor)])
            .send()
            .await
            .map_err(|_| SlackError::new("list_request_failed", None))
    }

    /// Upload verified artifact bytes. Callers must never provide filesystem paths.
    pub async fn upload_bytes(
        &self,
        filename: &str,
        bytes: Vec<u8>,
        channel: &str,
        thread_ts: Option<&str>,
    ) -> Result<String, SlackError> {
        if bytes.len() as u64 > crate::attachments::MAX_UPLOAD_BYTES {
            return Err(SlackError::new("upload_file_too_large", None));
        }
        if filename.is_empty() || filename.len() > 255 || filename.contains(['/', '\\']) {
            return Err(SlackError::new("invalid_upload_filename", None));
        }
        let request = serde_json::json!({"filename":filename,"length":bytes.len()});
        #[derive(Deserialize)]
        struct UploadUrl {
            upload_url: String,
            file_id: String,
        }
        let response = self
            .client
            .post(self.endpoint("files.getUploadURLExternal")?)
            .json(&request)
            .send()
            .await
            .map_err(|_| SlackError::new("upload_url_request_failed", None))?;
        let upload: UploadUrl = self.decode(response).await?;
        let upload_url = trusted_upload_url(&upload.upload_url)?;
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| SlackError::new("client_init_failed", None))?
            .post(upload_url)
            .body(bytes)
            .send()
            .await
            .map_err(|_| SlackError::new("upload_put_failed", None))?
            .error_for_status()
            .map_err(|_| SlackError::new("upload_put_failed", None))?;
        let mut complete = serde_json::json!({"files":[{"id":upload.file_id,"title":filename}],"channel_id":channel});
        if let Some(thread_ts) = thread_ts {
            complete["thread_ts"] = serde_json::Value::String(thread_ts.into());
        }
        let response = self
            .client
            .post(self.endpoint("files.completeUploadExternal")?)
            .json(&complete)
            .send()
            .await
            .map_err(|_| SlackError::new("upload_complete_failed", None))?;
        self.decode::<Empty>(response).await.map(|_| upload.file_id)
    }

    fn endpoint(&self, method: &str) -> Result<Url, SlackError> {
        self.base
            .join(method)
            .map_err(|_| SlackError::new("invalid_api_method", None))
    }

    async fn decode<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, SlackError> {
        let retry_after = retry_after(response.headers());
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("none")
            .to_owned();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(SlackError::new("rate_limited", retry_after));
        }
        if !status.is_success() {
            return Err(SlackError::new("http_error", retry_after));
        }
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| {
            SlackError::new(
                format!(
                    "invalid_api_response_status_{}_type_{}_bytes_{}",
                    status.as_u16(),
                    sanitize(&content_type).chars().take(40).collect::<String>(),
                    body.len()
                ),
                None,
            )
        })?;
        let shape = value.as_object().map_or_else(
            || "non_object".into(),
            |object| {
                let mut fields: Vec<_> = object.keys().map(String::as_str).collect();
                fields.sort_unstable();
                fields.join("_")
            },
        );
        let status_value: SlackStatus = serde_json::from_value(value.clone())
            .map_err(|_| SlackError::new(format!("invalid_api_shape_fields_{shape}"), None))?;
        if !status_value.ok {
            return Err(SlackError::new(
                status_value.error.unwrap_or_else(|| "unknown_error".into()),
                retry_after,
            ));
        }
        let envelope: SlackEnvelope<T> = serde_json::from_value(value)
            .map_err(|_| SlackError::new(format!("invalid_api_shape_fields_{shape}"), None))?;
        Ok(envelope.value)
    }
}

impl SlackError {
    pub(crate) fn new(code: impl Into<String>, retry_after: Option<Duration>) -> Self {
        Self {
            code: sanitize(&code.into()),
            retry_after,
        }
    }
}

impl SlackClient {
    pub(crate) async fn download(&self, url: Url) -> Result<reqwest::Response, SlackError> {
        self.client
            .get(url)
            .send()
            .await
            .map_err(|_| SlackError::new("attachment_download_failed", None))
    }
}

fn matches_query(query: Option<&str>, values: &[&str]) -> bool {
    query.is_none_or(|query| {
        values
            .iter()
            .any(|value| value.to_lowercase().contains(query))
    })
}

fn extend_page<T>(destination: &mut Vec<T>, mut page: Vec<T>, limit: usize) {
    page.truncate(limit.saturating_sub(destination.len()));
    destination.extend(page);
}

fn retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

pub fn sanitize(input: &str) -> String {
    let mut output = redact_slack_tokens(input);
    if let Some(index) = output.find("https://") {
        output.truncate(index);
        output.push_str("[private-url]");
    }
    output
}

fn redact_slack_tokens(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < bytes.len() {
        let slack_token = bytes[index..].starts_with(b"xapp-")
            || (bytes[index..].starts_with(b"xox")
                && index + 5 <= bytes.len()
                && matches!(
                    bytes[index + 3],
                    b'a' | b'b' | b'o' | b'p' | b'r' | b's' | b'c'
                )
                && bytes[index + 4] == b'-');
        let prefix_len = slack_token.then_some(5);
        if let Some(prefix_len) = prefix_len {
            output.push_str("[redacted]");
            index += prefix_len;
            while index < bytes.len()
                && matches!(bytes[index], b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_')
            {
                index += 1;
            }
        } else {
            let character = input[index..].chars().next().expect("valid utf8");
            output.push(character);
            index += character.len_utf8();
        }
    }
    output
}

fn trusted_upload_url(value: &str) -> Result<Url, SlackError> {
    let url = Url::parse(value).map_err(|_| SlackError::new("invalid_upload_url", None))?;
    if url.scheme() != "https"
        || !url
            .host_str()
            .is_some_and(|host| host == "slack.com" || host.ends_with(".slack.com"))
    {
        return Err(SlackError::new("untrusted_upload_url", None));
    }
    Ok(url)
}

pub fn public_users(users: Vec<SlackUser>) -> Vec<SlackUser> {
    users
        .into_iter()
        .filter(|user| !user.deleted && !user.is_bot)
        .collect()
}
pub fn public_channels(channels: Vec<SlackChannel>) -> Vec<SlackChannel> {
    channels
        .into_iter()
        .filter(|channel| !channel.is_archived)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts_tokens_and_urls() {
        assert_eq!(
            sanitize("xoxb-secret xapp-thing xoxp-private https://files.slack.com/private"),
            "[redacted] [redacted] [redacted] [private-url]"
        );
    }
    #[test]
    fn response_parsing_preserves_slack_error() {
        let parsed: SlackStatus =
            serde_json::from_str(r#"{"ok":false,"error":"missing_scope"}"#).unwrap();
        assert!(!parsed.ok);
        assert_eq!(parsed.error.as_deref(), Some("missing_scope"));
    }
    #[tokio::test]
    async fn list_users_paginates_past_inactive_users() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (expected_cursor, body) in [
                (
                    "cursor=",
                    r#"{"ok":true,"members":[{"id":"B1","is_bot":true},{"id":"D1","deleted":true}],"response_metadata":{"next_cursor":"next"}}"#,
                ),
                (
                    "cursor=next",
                    r#"{"ok":true,"members":[{"id":"U1"}],"response_metadata":{"next_cursor":""}}"#,
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                let length = stream.read(&mut request).await.unwrap();
                let request = std::str::from_utf8(&request[..length]).unwrap();
                assert!(request.starts_with("GET /users.list?limit=200&"));
                assert!(request.contains(expected_cursor));
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
        let client =
            SlackClient::with_base("xoxb-token".into(), &format!("http://{address}/")).unwrap();
        assert_eq!(
            client
                .list_users(1, None)
                .await
                .unwrap()
                .into_iter()
                .map(|user| user.id)
                .collect::<Vec<_>>(),
            ["U1"]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_channels_filters_archived_and_query_results() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let length = stream.read(&mut request).await.unwrap();
            let request = std::str::from_utf8(&request[..length]).unwrap();
            assert!(request.starts_with("GET /conversations.list?limit=200&cursor="));
            let body = r#"{"ok":true,"channels":[{"id":"C1","name":"General"},{"id":"C2","name":"random"},{"id":"C3","name":"general-archive","is_archived":true}],"response_metadata":{"next_cursor":""}}"#;
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
        });
        let client =
            SlackClient::with_base("xoxb-token".into(), &format!("http://{address}/")).unwrap();
        assert_eq!(
            client
                .list_channels(1, Some("GENERAL"))
                .await
                .unwrap()
                .into_iter()
                .map(|channel| channel.id)
                .collect::<Vec<_>>(),
            ["C1"]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn update_and_delete_messages_use_chat_endpoints() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (path, body) in [
                ("/chat.update", r#"{"ok":true,"channel":"C1","ts":"1.0"}"#),
                ("/chat.delete", r#"{"ok":true}"#),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                let length = stream.read(&mut request).await.unwrap();
                let request = std::str::from_utf8(&request[..length]).unwrap();
                assert!(request.starts_with(&format!("POST {path}")));
                assert!(request.contains("\"channel\":\"C1\""));
                assert!(request.contains("\"ts\":\"1.0\""));
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
        let client =
            SlackClient::with_base("xoxb-token".into(), &format!("http://{address}/")).unwrap();
        client
            .update_message("C1", "1.0", "thinking")
            .await
            .unwrap();
        client.delete_message("C1", "1.0").await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reaction_thread_lookup_uses_conversations_replies() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let length = stream.read(&mut request).await.unwrap();
            let request = std::str::from_utf8(&request[..length]).unwrap();
            assert!(request.starts_with("GET /conversations.replies?channel=C1&ts=2.0"));
            let body = r#"{"ok":true,"messages":[{"ts":"1.0"},{"ts":"2.0","thread_ts":"1.0"}]}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let client =
            SlackClient::with_base("xoxb-token".into(), &format!("http://{address}/")).unwrap();
        assert_eq!(
            client
                .message_thread_ts("C1", "2.0")
                .await
                .unwrap()
                .as_deref(),
            Some("1.0")
        );
        server.await.unwrap();
    }

    #[test]
    fn filters_inactive_resources() {
        assert_eq!(
            public_users(vec![
                SlackUser {
                    id: "U".into(),
                    name: "".into(),
                    real_name: "".into(),
                    deleted: false,
                    is_bot: false
                },
                SlackUser {
                    id: "B".into(),
                    name: "".into(),
                    real_name: "".into(),
                    deleted: false,
                    is_bot: true
                }
            ])
            .len(),
            1
        );
        assert_eq!(
            public_channels(vec![
                SlackChannel {
                    id: "C".into(),
                    name: "".into(),
                    is_archived: false,
                    is_member: false
                },
                SlackChannel {
                    id: "A".into(),
                    name: "".into(),
                    is_archived: true,
                    is_member: false
                }
            ])
            .len(),
            1
        );
    }
}
