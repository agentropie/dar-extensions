use crate::config::SecretToken;
use reqwest::{header, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use url::Url;

#[derive(Clone)]
pub struct MattermostClient {
    client: reqwest::Client,
    base: Url,
    token: SecretToken,
    users: Arc<RwLock<HashMap<String, User>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub code: &'static str,
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mattermost request failed ({})", self.code)
    }
}
impl std::error::Error for ApiError {}
impl ApiError {
    pub fn new(code: &'static str) -> Self {
        Self { code }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct User {
    pub id: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub first_name: String,
    #[serde(default)]
    pub last_name: String,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub delete_at: i64,
}
impl User {
    pub fn name(&self) -> String {
        let full = format!("{} {}", self.first_name, self.last_name)
            .trim()
            .to_owned();
        if !full.is_empty() {
            full
        } else if !self.nickname.is_empty() {
            self.nickname.clone()
        } else {
            self.username.clone()
        }
    }
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Post {
    pub id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub root_id: String,
    #[serde(default)]
    pub message: String,
    #[serde(default, rename = "type")]
    pub post_type: String,
    #[serde(default)]
    pub props: serde_json::Value,
    #[serde(default)]
    pub file_ids: Vec<String>,
    #[serde(default)]
    pub create_at: i64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Channel {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub team_id: String,
    #[serde(default)]
    pub delete_at: i64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Team {
    pub id: String,
    #[serde(default)]
    pub name: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FileInfo {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mime_type: String,
    #[serde(default)]
    pub size: u64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PostList {
    #[serde(default)]
    pub order: Vec<String>,
    #[serde(default)]
    pub posts: HashMap<String, Post>,
}

impl MattermostClient {
    pub fn new(base: &str, token: SecretToken) -> Result<Self, ApiError> {
        let mut base = Url::parse(base).map_err(|_| ApiError::new("invalid_base"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
        {
            return Err(ApiError::new("invalid_base"));
        }
        let path = format!(
            "{}/api/v4/",
            base.path()
                .trim_end_matches('/')
                .trim_end_matches("/api/v4")
        );
        base.set_path(&path);
        base.set_query(None);
        base.set_fragment(None);
        let mut headers = header::HeaderMap::new();
        let mut auth = header::HeaderValue::from_str(&format!("Bearer {}", token.expose()))
            .map_err(|_| ApiError::new("invalid_token"))?;
        auth.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ApiError::new("client_init"))?;
        Ok(Self {
            client,
            base,
            token,
            users: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    pub(crate) fn websocket_request(
        &self,
        connection_id: &str,
        sequence: u64,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, ApiError> {
        use tokio_tungstenite::tungstenite::{
            client::IntoClientRequest,
            http::{header::AUTHORIZATION, HeaderValue},
        };
        let mut url = self.endpoint("websocket")?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| ApiError::new("invalid_websocket_url"))?;
        if !connection_id.is_empty() {
            url.query_pairs_mut()
                .append_pair("connection_id", connection_id)
                .append_pair("sequence_number", &sequence.to_string());
        }
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|_| ApiError::new("invalid_websocket_request"))?;
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", self.token.expose()))
            .map_err(|_| ApiError::new("invalid_token"))?;
        auth.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, auth);
        Ok(request)
    }
    fn endpoint(&self, path: &str) -> Result<Url, ApiError> {
        self.base
            .join(path)
            .map_err(|_| ApiError::new("invalid_endpoint"))
    }
    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, ApiError> {
        let url = self.endpoint(path)?;
        let mut request = self.client.request(method, url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        self.exchange(
            || {
                request
                    .try_clone()
                    .ok_or_else(|| ApiError::new("unrepeatable_request"))
            },
            Duration::from_secs(30),
            json_body::<T>,
        )
        .await
    }
    async fn exchange<T, F: std::future::Future<Output = Result<T, BodyError>>>(
        &self,
        build: impl Fn() -> Result<reqwest::RequestBuilder, ApiError>,
        timeout: Duration,
        decode: impl Fn(reqwest::Response) -> F,
    ) -> Result<T, ApiError> {
        let deadline = tokio::time::Instant::now() + timeout;
        for attempt in 0..3 {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ApiError::new("deadline"));
            }
            let default_delay = Duration::from_millis(100 * (1 << attempt));
            let result = build()?.timeout(remaining).send().await;
            let delay = match result {
                Ok(response) if response.status().is_success() => match decode(response).await {
                    Ok(value) => return Ok(value),
                    Err(BodyError::Invalid) => return Err(ApiError::new("invalid_response")),
                    Err(BodyError::Network) if attempt == 2 => {
                        return Err(ApiError::new("network"))
                    }
                    Err(BodyError::Network) => default_delay,
                },
                Ok(response) => {
                    let status = response.status();
                    if !status.is_server_error() && status != StatusCode::TOO_MANY_REQUESTS {
                        return Err(ApiError::new(match status {
                            StatusCode::UNAUTHORIZED => "http_unauthorized",
                            StatusCode::FORBIDDEN => "http_forbidden",
                            StatusCode::NOT_FOUND => "http_not_found",
                            _ => "http_rejected",
                        }));
                    }
                    if attempt == 2 {
                        return Err(ApiError::new("retry_exhausted"));
                    }
                    response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs)
                        .unwrap_or(default_delay)
                }
                Err(_) if attempt == 2 => return Err(ApiError::new("network")),
                Err(_) => default_delay,
            };
            if delay >= deadline.saturating_duration_since(tokio::time::Instant::now()) {
                return Err(ApiError::new("deadline"));
            }
            tokio::time::sleep(delay).await;
        }
        Err(ApiError::new("retry_exhausted"))
    }
    pub async fn me(&self) -> Result<User, ApiError> {
        self.request(Method::GET, "users/me", None).await
    }
    pub async fn user(&self, id: &str) -> Result<User, ApiError> {
        self.users_by_ids(&[id.to_owned()])
            .await?
            .into_iter()
            .find(|user| user.id == id)
            .ok_or_else(|| ApiError::new("user_not_found"))
    }
    pub async fn users_by_ids(&self, ids: &[String]) -> Result<Vec<User>, ApiError> {
        let missing: Vec<_> = {
            let cache = self.users.read().await;
            ids.iter()
                .filter(|id| !cache.contains_key(id.as_str()))
                .cloned()
                .collect()
        };
        for batch in missing.chunks(100) {
            let fetched: Vec<User> = self
                .request(Method::POST, "users/ids", Some(serde_json::json!(batch)))
                .await?;
            let mut cache = self.users.write().await;
            for user in fetched {
                cache.insert(user.id.clone(), user);
            }
        }
        let cache = self.users.read().await;
        Ok(ids.iter().filter_map(|id| cache.get(id).cloned()).collect())
    }
    pub async fn username(&self, username: &str) -> Result<User, ApiError> {
        let user: User = self
            .request(
                Method::GET,
                &format!(
                    "users/username/{}",
                    segment(username.trim_start_matches('@'))
                ),
                None,
            )
            .await?;
        self.users
            .write()
            .await
            .insert(user.id.clone(), user.clone());
        Ok(user)
    }
    pub async fn list_users(
        &self,
        limit: usize,
        query: Option<&str>,
    ) -> Result<Vec<User>, ApiError> {
        let mut users = Vec::new();
        if limit == 0 {
            return Ok(users);
        }
        if let Some(query) = query {
            users = self
                .request(
                    Method::POST,
                    "users/search",
                    Some(serde_json::json!({"term":query,"limit":limit.min(1000)})),
                )
                .await?;
        } else {
            for page in 0..100 {
                let batch: Vec<User> = self
                    .request(
                        Method::GET,
                        &format!("users?page={page}&per_page=200"),
                        None,
                    )
                    .await?;
                let complete = batch.len() < 200;
                users.extend(batch.into_iter().filter(|user| user.delete_at == 0));
                if complete || users.len() >= limit {
                    break;
                }
            }
        }
        let query = query.map(str::to_lowercase);
        users.retain(|user| {
            user.delete_at == 0
                && query.as_ref().is_none_or(|q| {
                    user.username.to_lowercase().contains(q)
                        || user.name().to_lowercase().contains(q)
                })
        });
        users.truncate(limit);
        Ok(users)
    }
    pub async fn channel(&self, id: &str) -> Result<Channel, ApiError> {
        self.request(Method::GET, &format!("channels/{}", segment(id)), None)
            .await
    }
    pub async fn list_channels(
        &self,
        limit: usize,
        query: Option<&str>,
    ) -> Result<Vec<Channel>, ApiError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut channels: Vec<Channel> =
            self.request(Method::GET, "users/me/channels", None).await?;
        let query = query.map(str::to_lowercase);
        channels.retain(|channel| {
            channel.delete_at == 0
                && query.as_ref().is_none_or(|q| {
                    channel.name.to_lowercase().contains(q)
                        || channel.display_name.to_lowercase().contains(q)
                })
        });
        channels.truncate(limit);
        Ok(channels)
    }
    pub async fn teams(&self) -> Result<Vec<Team>, ApiError> {
        self.request(Method::GET, "users/me/teams", None).await
    }
    pub async fn open_dm(&self, bot: &str, user: &str) -> Result<Channel, ApiError> {
        self.request(
            Method::POST,
            "channels/direct",
            Some(serde_json::json!([bot, user])),
        )
        .await
    }
    pub async fn upload_bytes(
        &self,
        channel: &str,
        filename: &str,
        bytes: Vec<u8>,
    ) -> Result<Vec<FileInfo>, ApiError> {
        if filename.is_empty()
            || filename.contains(['/', '\\'])
            || filename == "."
            || filename == ".."
            || filename.chars().any(char::is_control)
        {
            return Err(ApiError::new("invalid_filename"));
        }
        let mut url = self.endpoint("files")?;
        url.query_pairs_mut().append_pair("channel_id", channel);
        let upload = self
            .exchange(
                || {
                    let form = reqwest::multipart::Form::new()
                        .text("channel_id", channel.to_owned())
                        .part(
                            "files",
                            reqwest::multipart::Part::bytes(bytes.clone())
                                .file_name(filename.to_owned()),
                        );
                    Ok(self.client.post(url.clone()).multipart(form))
                },
                Duration::from_secs(120),
                json_body::<Upload>,
            )
            .await?;
        #[derive(Deserialize)]
        struct Upload {
            file_infos: Vec<FileInfo>,
        }
        let files = upload.file_infos;
        if files.is_empty() {
            return Err(ApiError::new("missing_uploaded_file"));
        }
        Ok(files)
    }
    pub async fn channel_posts(
        &self,
        channel: &str,
        before: Option<&str>,
        since: Option<i64>,
        limit: usize,
    ) -> Result<PostList, ApiError> {
        let mut url = self.endpoint(&format!("channels/{}/posts", segment(channel)))?;
        url.query_pairs_mut()
            .append_pair("per_page", &limit.min(200).to_string());
        if let Some(before) = before {
            url.query_pairs_mut().append_pair("before", before);
        }
        if let Some(since) = since {
            url.query_pairs_mut()
                .append_pair("since", &since.to_string());
        }
        self.exchange(
            || Ok(self.client.get(url.clone())),
            Duration::from_secs(30),
            json_body::<PostList>,
        )
        .await
    }
    pub async fn thread(&self, root: &str) -> Result<PostList, ApiError> {
        self.request(
            Method::GET,
            &format!("posts/{}/thread", segment(root)),
            None,
        )
        .await
    }
    pub async fn add_reaction(&self, bot: &str, post: &str, emoji: &str) -> Result<(), ApiError> {
        self.request::<serde_json::Value>(
            Method::POST,
            "reactions",
            Some(serde_json::json!({"user_id":bot,"post_id":post,"emoji_name":emoji})),
        )
        .await
        .map(|_| ())
    }
    pub async fn remove_reaction(
        &self,
        bot: &str,
        post: &str,
        emoji: &str,
    ) -> Result<(), ApiError> {
        self.request::<serde_json::Value>(
            Method::DELETE,
            &format!(
                "users/{}/posts/{}/reactions/{}",
                segment(bot),
                segment(post),
                segment(emoji)
            ),
            None,
        )
        .await
        .map(|_| ())
    }
    pub async fn typing(
        &self,
        bot: &str,
        channel: &str,
        root: Option<&str>,
    ) -> Result<(), ApiError> {
        let mut body = serde_json::json!({"channel_id":channel});
        if let Some(root) = root {
            body["parent_id"] = root.into();
        }
        self.request::<serde_json::Value>(
            Method::POST,
            &format!("users/{}/typing", segment(bot)),
            Some(body),
        )
        .await
        .map(|_| ())
    }
    pub async fn file_info(&self, id: &str) -> Result<FileInfo, ApiError> {
        self.request(Method::GET, &format!("files/{}/info", segment(id)), None)
            .await
    }
    pub async fn download_file(&self, id: &str) -> Result<reqwest::Response, ApiError> {
        self.download_url(self.endpoint(&format!("files/{}", segment(id)))?)
            .await
    }
    pub async fn download_url(&self, url: Url) -> Result<reqwest::Response, ApiError> {
        if url.origin() != self.base.origin()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(ApiError::new("untrusted_download_origin"));
        }
        self.exchange(
            || Ok(self.client.get(url.clone())),
            Duration::from_secs(120),
            |response| std::future::ready(Ok(response)),
        )
        .await
    }
    pub async fn patch_post(&self, id: &str, text: &str) -> Result<(), ApiError> {
        self.request::<Post>(
            Method::PUT,
            &format!("posts/{}/patch", segment(id)),
            Some(serde_json::json!({"message":text})),
        )
        .await
        .map(|_| ())
    }
    pub async fn delete_post(&self, id: &str) -> Result<(), ApiError> {
        self.request::<serde_json::Value>(Method::DELETE, &format!("posts/{}", segment(id)), None)
            .await
            .map(|_| ())
    }
    pub async fn create_post(
        &self,
        channel: &str,
        text: &str,
        root: Option<&str>,
        files: &[String],
    ) -> Result<Post, ApiError> {
        let mut body = serde_json::json!({"channel_id":channel,"message":text,"file_ids":files});
        if let Some(root) = root {
            body["root_id"] = root.into();
        }
        self.request(Method::POST, "posts", Some(body)).await
    }
}

enum BodyError {
    Network,
    Invalid,
}
async fn json_body<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, BodyError> {
    let bytes = response.bytes().await.map_err(|_| BodyError::Network)?;
    serde_json::from_slice(&bytes).map_err(|_| BodyError::Invalid)
}

fn segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

// Bounded, same-server Mattermost REST operations with safe error values.
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn server(
        statuses: Vec<u16>,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for status in statuses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 65536];
                let n = socket.read(&mut request).await.unwrap();
                requests.push(String::from_utf8_lossy(&request[..n]).into_owned());
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (base, task)
    }

    #[tokio::test]
    async fn creating_a_thread_reply_preserves_text_files_and_root() {
        let (base, server) = server(
            vec![201],
            r#"{"id":"postone0000000000000000000","channel_id":"channelone0000000000000000","message":"hello"}"#,
        )
        .await;
        let client = MattermostClient::new(
            &base,
            crate::config::SecretToken::new("invented-token".into()),
        )
        .unwrap();
        let post = client
            .create_post(
                "channelone0000000000000000",
                "hello",
                Some("rootone0000000000000000000"),
                &["fileone0000000000000000000".into()],
            )
            .await
            .unwrap();
        assert_eq!(post.id, "postone0000000000000000000");
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /api/v4/posts "));
        let body: serde_json::Value =
            serde_json::from_str(requests[0].split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"channel_id":"channelone0000000000000000","message":"hello","root_id":"rootone0000000000000000000","file_ids":["fileone0000000000000000000"]})
        );
    }
    #[tokio::test]
    async fn retries_transient_errors_three_times_but_never_retries_forbidden() {
        for (statuses, expected) in [
            (vec![503, 429, 201], "postone0000000000000000000"),
            (vec![403], "http_forbidden"),
        ] {
            let (base, task) = server(statuses, r#"{"id":"postone0000000000000000000"}"#).await;
            let client = MattermostClient::new(
                &base,
                crate::config::SecretToken::new("invented-private-token".into()),
            )
            .unwrap();
            let result = client
                .create_post("channelone0000000000000000", "hello", None, &[])
                .await;
            match result {
                Ok(post) => assert_eq!(post.id, expected),
                Err(error) => {
                    assert_eq!(error.code, expected);
                    assert!(!format!("{error:?} {error}").contains("invented-private-token"));
                    assert!(!format!("{error:?} {error}").contains(&base));
                }
            }
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn user_lookup_uses_batched_ids_and_reuses_cached_identity() {
        let (base, server) = server(
            vec![200],
            r#"[{"id":"reader00000000000000000000","username":"sage","first_name":"Sage","last_name":"Green"}]"#,
        )
        .await;
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let user = client.user("reader00000000000000000000").await.unwrap();
        assert_eq!(user.name(), "Sage Green");
        assert_eq!(
            client
                .user("reader00000000000000000000")
                .await
                .unwrap()
                .username,
            "sage"
        );
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("POST /api/v4/users/ids "));
        assert!(requests[0].ends_with("[\"reader00000000000000000000\"]"));
    }

    #[tokio::test]
    async fn listings_include_bots_and_filter_case_insensitively() {
        let (base, users) = server(vec![200], r#"[{"id":"botone00000000000000000000","username":"Helper","is_bot":true},{"id":"gone0000000000000000000000","username":"helper_old","delete_at":9}]"#).await;
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let listed = client.list_users(100, Some("HELPER")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert!(listed[0].is_bot);
        assert_eq!(listed[0].id, "botone00000000000000000000");
        assert!(users.await.unwrap()[0].starts_with("POST /api/v4/users/search "));
        let (base, channels) = server(vec![200], r#"[{"id":"one00000000000000000000000","name":"general","display_name":"Project Room","type":"P"},{"id":"two00000000000000000000000","name":"random"}]"#).await;
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let listed = client.list_channels(1, Some("PROJECT")).await.unwrap();
        assert_eq!(listed[0].id, "one00000000000000000000000");
        assert_eq!(listed.len(), 1);
        assert!(channels.await.unwrap()[0].starts_with("GET /api/v4/users/me/channels "));
    }

    #[tokio::test]
    async fn file_upload_uses_multipart_and_keeps_channel_target() {
        use crate::test_http::{Response, Server};
        let mut server = Server::start(vec![Response::json(serde_json::json!({"file_infos":[{"id":"fileone0000000000000000000","name":"note.txt","size":5,"mime_type":"text/plain"}]}))]).await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let files = client
            .upload_bytes("channelone0000000000000000", "note.txt", b"hello".to_vec())
            .await
            .unwrap();
        assert_eq!(files[0].id, "fileone0000000000000000000");
        let request = server.next().await;
        assert!(request
            .path
            .starts_with("/api/v4/files?channel_id=channelone0000000000000000"));
        assert!(request.headers.contains("multipart/form-data"));
        let body = String::from_utf8(request.body).unwrap();
        assert!(body.contains("name=\"channel_id\"\r\n\r\nchannelone"));
        assert!(body.contains("filename=\"note.txt\""));
        assert!(body.contains("hello"));
        server.finish().await;
    }

    #[tokio::test]
    async fn history_reactions_and_typing_use_the_expected_conversation() {
        use crate::test_http::{Response, Server};
        let mut server = Server::start(vec![Response::json(serde_json::json!({"order":["earlier0000000000000000000"],"posts":{"earlier0000000000000000000":{"id":"earlier0000000000000000000","message":"old"}}})), Response::json(serde_json::json!({})), Response::json(serde_json::json!({})), Response::json(serde_json::json!({}))]).await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let history = client
            .channel_posts(
                "room0000000000000000000000",
                Some("current0000000000000000000"),
                None,
                20,
            )
            .await
            .unwrap();
        assert_eq!(history.posts["earlier0000000000000000000"].message, "old");
        assert_eq!(
            server.next().await.path,
            "/api/v4/channels/room0000000000000000000000/posts?per_page=20&before=current0000000000000000000"
        );
        client
            .add_reaction(
                "helper00000000000000000000",
                "source00000000000000000000",
                "eyes",
            )
            .await
            .unwrap();
        let reaction = server.next().await;
        assert_eq!(reaction.path, "/api/v4/reactions");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&reaction.body).unwrap(),
            serde_json::json!({"user_id":"helper00000000000000000000","post_id":"source00000000000000000000","emoji_name":"eyes"})
        );
        client
            .remove_reaction(
                "helper00000000000000000000",
                "source00000000000000000000",
                "eyes",
            )
            .await
            .unwrap();
        assert_eq!(
            server.next().await.path,
            "/api/v4/users/helper00000000000000000000/posts/source00000000000000000000/reactions/eyes"
        );
        client
            .typing(
                "helper00000000000000000000",
                "room0000000000000000000000",
                Some("root0000000000000000000000"),
            )
            .await
            .unwrap();
        let typing = server.next().await;
        assert_eq!(
            typing.path,
            "/api/v4/users/helper00000000000000000000/typing"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&typing.body).unwrap(),
            serde_json::json!({"channel_id":"room0000000000000000000000","parent_id":"root0000000000000000000000"})
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn download_stays_on_the_configured_origin_and_never_follows_redirects() {
        use crate::test_http::{Response, Server};
        let mut server = Server::start(vec![
            Response::bytes(b"document".to_vec()),
            Response {
                status: 302,
                headers: vec![(
                    "Location".into(),
                    "https://other.example.com/private".into(),
                )],
                body: Vec::new(),
            },
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let error = client
            .download_url(Url::parse("https://other.example.com/private").unwrap())
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, "untrusted_download_origin");
        let bytes = client
            .download_file("document000000000000000000")
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"document");
        assert_eq!(
            server.next().await.path,
            "/api/v4/files/document000000000000000000"
        );
        assert_eq!(
            client
                .download_file("redirect000000000000000000")
                .await
                .err()
                .unwrap()
                .code,
            "http_rejected"
        );
        assert_eq!(
            server.next().await.path,
            "/api/v4/files/redirect000000000000000000"
        );
        server.finish().await;
    }
    #[tokio::test]
    async fn a_hung_body_expires_without_blocking_other_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (waiting, accepted) = tokio::sync::oneshot::channel();
        let release = tokio_util::sync::CancellationToken::new();
        let released = release.clone();
        let server = tokio::spawn(async move {
            let (mut hung, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(hung.read(&mut request).await.unwrap() > 0);
            hung.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 9999\r\nConnection: close\r\n\r\n{",
            )
            .await
            .unwrap();
            waiting.send(()).unwrap();
            let (mut next, _) = listener.accept().await.unwrap();
            assert!(next.read(&mut request).await.unwrap() > 0);
            let body = r#"{"id":"helper00000000000000000000","username":"helper"}"#;
            next.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            released.cancelled().await;
        });
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let other = client.clone();
        let hung = tokio::spawn(async move {
            other
                .create_post("room0000000000000000000000", "hello", None, &[])
                .await
        });
        accepted.await.unwrap();
        assert_eq!(client.me().await.unwrap().username, "helper");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(tokio::time::timeout(Duration::from_millis(1), hung)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        release.cancel();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn retries_share_one_deadline_instead_of_starting_another_thirty_seconds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (waiting, accepted) = tokio::sync::oneshot::channel();
        let release = tokio_util::sync::CancellationToken::new();
        let released = release.clone();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(first.read(&mut request).await.unwrap() > 0);
            first.write_all(b"HTTP/1.1 503 Test\r\nContent-Length: 2\r\nConnection: close\r\nRetry-After: 1\r\n\r\n{}").await.unwrap();
            drop(first);
            let (mut retry, _) = listener.accept().await.unwrap();
            assert!(retry.read(&mut request).await.unwrap() > 0);
            waiting.send(()).unwrap();
            released.cancelled().await;
        });
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let request = tokio::spawn(async move {
            client
                .create_post("room0000000000000000000000", "hello", None, &[])
                .await
        });
        accepted.await.unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(29)).await;
        let result = tokio::time::timeout(Duration::from_millis(10), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().code, "deadline");
        release.cancel();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn a_file_body_has_the_longer_but_finite_two_minute_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let release = tokio_util::sync::CancellationToken::new();
        let released = release.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 9999\r\nConnection: close\r\n\r\n{")
                .await
                .unwrap();
            released.cancelled().await;
        });
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let response = client
            .download_file("document000000000000000000")
            .await
            .unwrap();
        let reading = tokio::spawn(async move { response.bytes().await });
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(
            !reading.is_finished(),
            "file operations must not use the ordinary 30s budget"
        );
        tokio::time::advance(Duration::from_secs(90)).await;
        assert!(tokio::time::timeout(Duration::from_millis(1), reading)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        release.cancel();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn interrupted_bodies_retry_but_complete_malformed_json_does_not() {
        use crate::test_http::{Response, Server};
        let mut server = Server::start(vec![
            Response {
                status: 200,
                headers: vec![("Content-Length".into(), "9999".into())],
                body: b"{".to_vec(),
            },
            Response::json(serde_json::json!({"id":"complete000000000000000000"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        assert_eq!(
            client
                .create_post("room0000000000000000000000", "hello", None, &[])
                .await
                .unwrap()
                .id,
            "complete000000000000000000"
        );
        assert_eq!(server.next().await.path, "/api/v4/posts");
        assert_eq!(server.next().await.path, "/api/v4/posts");
        server.finish().await;
        let mut server = Server::start(vec![Response::bytes(b"{".to_vec())]).await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        assert_eq!(
            client
                .create_post("room0000000000000000000000", "hello", None, &[])
                .await
                .unwrap_err()
                .code,
            "invalid_response"
        );
        assert_eq!(server.next().await.path, "/api/v4/posts");
        server.finish().await;
    }
}
