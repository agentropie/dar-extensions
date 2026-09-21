//! WhatsApp Business Cloud API channel for dar agents.

use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
    deliver::{DeliverySink, Destination},
    tools::{ToolExecutor, ToolOutcome, ToolRegistryHandle, ToolSpec, TOOL_REGISTRY_SERVICE},
    ConfigStore, Extension, RegisterCtx, StartCtx,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Semaphore};

mod graph;
mod session;
mod webhook;

use graph::Graph;
use session::{is_reset, valid_wa_id, SessionStore, SessionsConfig, EXPIRED_NOTICE, RESET_REPLY};
use webhook::{Inbound, WebhookState};

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct WhatsAppConfig {
    phone_number_id: Option<String>,
    access_token: Option<String>,
    app_secret: Option<String>,
    verify_token: Option<String>,
    api_version: String,
    allowed_users: Vec<String>,
    backend: Option<String>,
    tool_status: bool,
    sessions: SessionsConfig,
    webhook: WebhookConfig,
}
impl Default for WhatsAppConfig {
    fn default() -> Self {
        Self {
            phone_number_id: None,
            access_token: None,
            app_secret: None,
            verify_token: None,
            api_version: "v20.0".into(),
            allowed_users: vec![],
            backend: None,
            tool_status: true,
            sessions: SessionsConfig::default(),
            webhook: WebhookConfig::default(),
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct WebhookConfig {
    bind: String,
    port: u16,
    path: String,
    public_url: Option<String>,
}
impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".into(),
            port: 8090,
            path: "/whatsapp/webhook".into(),
            public_url: None,
        }
    }
}

pub struct WhatsAppExtension;
pub fn extension() -> Box<dyn Extension> {
    Box::new(WhatsAppExtension)
}
impl Extension for WhatsAppExtension {
    fn id(&self) -> &'static str {
        "whatsapp"
    }
    fn agent_singleton(&self) -> bool {
        true
    }
    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config)?;
            let phone = credential(&cfg.phone_number_id, "PHONE_NUMBER_ID")
                .context("whatsapp.phone_number_id or WHATSAPP_PHONE_NUMBER_ID is required")?;
            let token = credential(&cfg.access_token, "ACCESS_TOKEN")
                .context("whatsapp.access_token or WHATSAPP_ACCESS_TOKEN is required")?;
            let graph = Arc::new(Graph::new(&phone, token, &cfg.api_version)?);
            let tool = Arc::new(WhatsAppSendTool {
                graph,
                allowed: cfg.allowed_users.clone(),
            });
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn ToolRegistryHandle>(TOOL_REGISTRY_SERVICE)
            {
                registry.register_tool(send_spec(), tool.clone())?;
            }
            ctx.services.service::<dyn DeliverySink>("whatsapp", tool)?;
            Ok(())
        })
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config)?;
            let phone = credential(&cfg.phone_number_id, "PHONE_NUMBER_ID")
                .context("whatsapp phone number id missing at start")?;
            let token = credential(&cfg.access_token, "ACCESS_TOKEN")
                .context("whatsapp access token missing at start")?;
            if !cfg.webhook.path.starts_with('/') {
                bail!("whatsapp.webhook.path must start with '/'");
            }
            let listener =
                tokio::net::TcpListener::bind(format!("{}:{}", cfg.webhook.bind, cfg.webhook.port))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to bind WhatsApp webhook on {}:{}",
                            cfg.webhook.bind, cfg.webhook.port
                        )
                    })?;
            let address = listener.local_addr()?;
            let session_root = ctx.paths.data_dir("whatsapp")?.join("sessions");
            std::fs::create_dir_all(&session_root)?;
            let graph = Arc::new(Graph::new(&phone, token, &cfg.api_version)?);
            let (tx, rx) = mpsc::channel(256);
            let app_secret = credential(&cfg.app_secret, "APP_SECRET");
            let verify_token = credential(&cfg.verify_token, "VERIFY_TOKEN");
            if app_secret.is_none() {
                tracing::warn!("whatsapp app_secret is unset; inbound webhook will return 503");
            }
            if verify_token.is_none() {
                tracing::warn!(
                    "whatsapp verify_token is unset; webhook verification will return 503"
                );
            }
            dar_extension_sdk::log::event(
                "-",
                "whatsapp",
                &format!(
                    "whatsapp webhook listening on http://{}{}",
                    address, cfg.webhook.path
                ),
            );
            if let Some(origin) = cfg
                .webhook
                .public_url
                .as_deref()
                .filter(|v| !v.trim().is_empty())
            {
                dar_extension_sdk::log::event(
                    "-",
                    "whatsapp",
                    &format!(
                        "whatsapp webhook URL for Meta: {}{} (verify token configured: {})",
                        origin.trim_end_matches('/'),
                        cfg.webhook.path,
                        if verify_token.is_some() { "yes" } else { "no" }
                    ),
                );
            } else {
                dar_extension_sdk::log::event("-", "whatsapp", &format!("whatsapp: expose port {} publicly over HTTPS (e.g. tailscale funnel, or a reverse proxy) and register <public>{} in the Meta app", address.port(), cfg.webhook.path));
            }
            let state = WebhookState {
                verify_token,
                app_secret,
                phone_number_id: phone,
                inbound: tx,
                dedup: Default::default(),
            };
            let server_shutdown = ctx.shutdown.clone();
            let app = webhook::router(state, &cfg.webhook.path);
            tokio::spawn(async move {
                if let Err(err) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let mut shutdown = server_shutdown;
                        shutdown.cancelled().await;
                    })
                    .await
                {
                    tracing::error!(error=%err, "WhatsApp webhook server stopped");
                }
            });
            let dispatch_shutdown = ctx.shutdown.clone();
            tokio::spawn(async move {
                dispatcher(ctx, cfg, graph, session_root, rx, dispatch_shutdown).await;
            });
            Ok(())
        })
    }
}

fn parse_config(config: &ConfigStore) -> Result<WhatsAppConfig> {
    Ok(config
        .get("whatsapp")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default())
}
fn credential(value: &Option<String>, name: &str) -> Option<String> {
    value.clone().filter(|v| !v.trim().is_empty()).or_else(|| {
        std::env::var(format!("WHATSAPP_{name}"))
            .ok()
            .filter(|v| !v.trim().is_empty())
    })
}
fn authorized(wa_id: &str, allowed: &[String]) -> bool {
    allowed.is_empty() || allowed.iter().any(|id| id == wa_id)
}

fn send_spec() -> ToolSpec {
    ToolSpec::new("whatsapp_send_message", "Send a WhatsApp text message to an exact WhatsApp user id.", json!({"type":"object","additionalProperties":false,"properties":{"wa_id":{"type":"string","description":"Exact WhatsApp wa_id (digits only)."},"text":{"type":"string","minLength":1}},"required":["wa_id","text"]})).writes()
}
struct WhatsAppSendTool {
    graph: Arc<Graph>,
    allowed: Vec<String>,
}
#[async_trait]
impl ToolExecutor for WhatsAppSendTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        let Some(wa_id) = args
            .get("wa_id")
            .and_then(Value::as_str)
            .filter(|id| valid_wa_id(id))
        else {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "whatsapp_send_message requires digits-only 'wa_id'",
                None::<String>,
            ));
        };
        let Some(text) = args
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        else {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "whatsapp_send_message requires non-empty 'text'",
                None::<String>,
            ));
        };
        if !authorized(wa_id, &self.allowed) {
            return Ok(ToolOutcome::error_code(
                "not_authorized",
                "WhatsApp recipient is not authorized",
                None::<String>,
            ));
        }
        match self.graph.send_text(wa_id, text, None).await {
            Ok(()) => Ok(ToolOutcome::ok(format!(
                "accepted WhatsApp message for {wa_id}"
            ))),
            Err(err) => Ok(ToolOutcome::error_code(
                "send_failed",
                format!("WhatsApp send failed: {err:#}"),
                None::<String>,
            )),
        }
    }
}
#[async_trait]
impl DeliverySink for WhatsAppSendTool {
    async fn deliver(&self, dest: &Destination, text: &str) -> Result<()> {
        let wa_id = dest
            .user
            .as_deref()
            .context("whatsapp delivery requires user")?;
        let outcome = self.execute(json!({"wa_id":wa_id,"text":text})).await?;
        if outcome.is_error {
            bail!("{}", outcome.text);
        }
        Ok(())
    }
}

async fn dispatcher(
    ctx: StartCtx,
    cfg: WhatsAppConfig,
    graph: Arc<Graph>,
    session_root: std::path::PathBuf,
    mut rx: mpsc::Receiver<Inbound>,
    mut shutdown: dar_extension_sdk::ShutdownToken,
) {
    let mut state = DispatcherState {
        statuses: HashMap::new(),
        sessions: HashMap::new(),
        status_slots: Arc::new(Semaphore::new(8)),
    };
    loop {
        tokio::select! { _ = shutdown.cancelled() => break, incoming = rx.recv() => match incoming { Some(inbound) => process(&ctx, &cfg, &graph, &session_root, inbound, &mut state).await, None => break } }
    }
    for (_, connection) in state.sessions {
        let _ = connection.session.close().await;
    }
}

struct DispatcherState {
    statuses: HashMap<String, Instant>,
    sessions: HashMap<String, ChatConn>,
    status_slots: Arc<Semaphore>,
}

struct ChatConn {
    session: Box<dyn dar_extension_sdk::chat::ChatSession>,
    events: mpsc::Receiver<ChatEvent>,
}

async fn open_session(
    ctx: &StartCtx,
    cfg: &WhatsAppConfig,
    session_dir: &Path,
) -> Result<ChatConn> {
    let backend_id = dar_extension_sdk::chat::resolve_agent_backend(ctx, cfg.backend.as_deref());
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
    let (tx, events) = mpsc::channel(256);
    let session = backend
        .open(
            dar_extension_sdk::chat::agent_session_params(ctx, session_dir).build(),
            tx,
        )
        .await?;
    Ok(ChatConn { session, events })
}

async fn drop_session(sessions: &mut HashMap<String, ChatConn>, wa_id: &str) {
    if let Some(connection) = sessions.remove(wa_id) {
        let _ = connection.session.close().await;
    }
}
async fn process(
    ctx: &StartCtx,
    cfg: &WhatsAppConfig,
    graph: &Arc<Graph>,
    root: &Path,
    inbound: Inbound,
    state: &mut DispatcherState,
) {
    if !authorized(&inbound.wa_id, &cfg.allowed_users) {
        let _ = graph
            .send_text(&inbound.wa_id, "Not authorized.", Some(&inbound.wamid))
            .await;
        return;
    }
    let store = match SessionStore::new(root, &inbound.wa_id) {
        Ok(store) => store,
        Err(_) => return,
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .unwrap_or(0);
    if is_reset(&inbound.text) {
        drop_session(&mut state.sessions, &inbound.wa_id).await;
        if store.reset(now).is_ok() {
            let _ = graph
                .send_text(&inbound.wa_id, RESET_REPLY, Some(&inbound.wamid))
                .await;
        }
        return;
    }
    let prepared = match store.prepare(cfg.sessions.idle_minutes, now) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error=%err, "whatsapp session prepare failed");
            let _ = graph
                .send_text(
                    &inbound.wa_id,
                    "Failed to prepare session.",
                    Some(&inbound.wamid),
                )
                .await;
            return;
        }
    };
    if prepared.rotated {
        drop_session(&mut state.sessions, &inbound.wa_id).await;
        let _ = graph
            .send_text(&inbound.wa_id, EXPIRED_NOTICE, Some(&inbound.wamid))
            .await;
    }
    if !state.sessions.contains_key(&inbound.wa_id) {
        match open_session(ctx, cfg, &prepared.session_dir).await {
            Ok(connection) => {
                state.sessions.insert(inbound.wa_id.clone(), connection);
            }
            Err(err) => {
                let _ = graph
                    .send_text(
                        &inbound.wa_id,
                        &format!("(turn failed: {err:#})"),
                        Some(&inbound.wamid),
                    )
                    .await;
                return;
            }
        }
    }
    let result = run_turn(
        state
            .sessions
            .get_mut(&inbound.wa_id)
            .expect("session inserted"),
        cfg,
        graph,
        &inbound,
        &mut state.statuses,
        &state.status_slots,
        ctx.shutdown.clone(),
    )
    .await;
    if result.is_err() {
        drop_session(&mut state.sessions, &inbound.wa_id).await;
    }
    if ctx.shutdown.is_cancelled() {
        return;
    }
    let reply = result.unwrap_or_else(|err| format!("(turn failed: {err:#})"));
    let _ = graph
        .send_text(
            &inbound.wa_id,
            &adapt_markdown(&reply),
            Some(&inbound.wamid),
        )
        .await;
}
async fn run_turn(
    connection: &mut ChatConn,
    cfg: &WhatsAppConfig,
    graph: &Arc<Graph>,
    inbound: &Inbound,
    statuses: &mut HashMap<String, Instant>,
    status_slots: &Arc<Semaphore>,
    mut shutdown: dar_extension_sdk::ShutdownToken,
) -> Result<String> {
    connection.session.send_turn(inbound.text.clone()).await?;
    let (stop_typing, typing_stop) = watch::channel(false);
    let typing_graph = Arc::clone(graph);
    let typing_id = inbound.wamid.clone();
    let typing = tokio::spawn(async move {
        typing_loop(typing_stop, Duration::from_secs(20), || async {
            let _ =
                tokio::time::timeout(Duration::from_secs(5), typing_graph.typing(&typing_id)).await;
        })
        .await;
    });
    let mut answer = String::new();
    let mut failure = None;
    loop {
        let event = tokio::select! {
            _ = shutdown.cancelled() => {
                failure = Some("shutdown requested".into());
                break;
            }
            event = connection.events.recv() => event,
        };
        let Some(event) = event else {
            failure = Some("backend event stream closed".into());
            break;
        };
        match event {
            ChatEvent::Delta {
                role: ChatRole::Assistant,
                text,
            } => answer.push_str(&text),
            ChatEvent::ToolCall { name, args, .. }
                if tool_status_allowed(
                    cfg.tool_status,
                    statuses,
                    &inbound.wa_id,
                    Instant::now(),
                ) =>
            {
                if let Ok(permit) = Arc::clone(status_slots).try_acquire_owned() {
                    let graph = Arc::clone(graph);
                    let wa_id = inbound.wa_id.clone();
                    let text = format!("⚙ {}", tool_preview(&name, &args));
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = tokio::time::timeout(
                            Duration::from_secs(5),
                            graph.send_text(&wa_id, &text, None),
                        )
                        .await;
                    });
                }
            }
            ChatEvent::TurnFinished { ok, error } => {
                if !ok {
                    failure = Some(error.unwrap_or_else(|| "unknown".into()));
                }
                break;
            }
            ChatEvent::SessionClosed { error } => {
                failure = Some(error.unwrap_or_else(|| "backend closed".into()));
                break;
            }
            _ => {}
        }
    }
    let _ = stop_typing.send(true);
    let _ = typing.await;
    if let Some(error) = failure {
        bail!("{error}");
    }
    if answer.trim().is_empty() {
        Ok("(no response)".into())
    } else {
        Ok(answer)
    }
}
fn status_allowed(last: &mut HashMap<String, Instant>, wa_id: &str, now: Instant) -> bool {
    match last.get(wa_id) {
        Some(previous) if now.duration_since(*previous) < Duration::from_secs(2) => false,
        _ => {
            last.insert(wa_id.to_string(), now);
            true
        }
    }
}
fn tool_status_allowed(
    enabled: bool,
    last: &mut HashMap<String, Instant>,
    wa_id: &str,
    now: Instant,
) -> bool {
    enabled && status_allowed(last, wa_id, now)
}
async fn typing_loop<F, Fut>(mut stop: watch::Receiver<bool>, every: Duration, mut send: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        let send = send();
        tokio::pin!(send);
        tokio::select! {
            _ = stop.changed() => break,
            _ = &mut send => {}
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(every) => {}
        }
    }
}
fn tool_preview(name: &str, args: &str) -> String {
    let value: Value = serde_json::from_str(args).unwrap_or_default();
    let target = value
        .as_object()
        .and_then(|o| {
            [
                "path",
                "file",
                "file_path",
                "filename",
                "cmd",
                "command",
                "query",
                "pattern",
                "url",
                "name",
                "target",
            ]
            .iter()
            .find_map(|k| o.get(*k).and_then(Value::as_str))
        })
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(80).collect::<String>());
    target
        .map(|target| format!("{name} · {target}"))
        .unwrap_or_else(|| name.to_string())
}
pub fn adapt_markdown(text: &str) -> String {
    text.lines()
        .map(|line| {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') {
                trimmed
                    .trim_start_matches('#')
                    .trim_start()
                    .replace("**", "*")
            } else {
                line.replace("**", "*")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dar_extension_sdk::chat::{BoxFuture, ChatSession, ChatSessionParams};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeBackend;
    struct FakeSession {
        events: mpsc::Sender<ChatEvent>,
    }
    impl ChatBackend for FakeBackend {
        fn open<'a>(
            &'a self,
            _params: ChatSessionParams,
            events: mpsc::Sender<ChatEvent>,
        ) -> BoxFuture<'a, Result<Box<dyn ChatSession>>> {
            Box::pin(async move { Ok(Box::new(FakeSession { events }) as Box<dyn ChatSession>) })
        }
    }
    impl ChatSession for FakeSession {
        fn send_turn(&mut self, prompt: String) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                assert_eq!(prompt, "hello");
                self.events
                    .send(ChatEvent::Delta {
                        role: ChatRole::Assistant,
                        text: "agent reply".into(),
                    })
                    .await?;
                self.events
                    .send(ChatEvent::TurnFinished {
                        ok: true,
                        error: None,
                    })
                    .await?;
                Ok(())
            })
        }
        fn abort(&mut self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn test_ctx(root: &Path) -> (StartCtx, watch::Sender<bool>) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let paths = host_api::HostPaths::new(root).unwrap();
        let mut register = host_api::RegisterCtx {
            bus: host_api::EventBus::new(),
            http: host_api::HttpRegistry::disabled(),
            foreground: host_api::ForegroundRegistry::default(),
            services: host_api::ServiceRegistry::default(),
            paths: paths.clone(),
            config: host_api::ConfigStore::default(),
            shutdown: host_api::ShutdownToken::new(shutdown_rx.clone()),
        };
        register
            .services
            .service::<dyn ChatBackend>("pi", Arc::new(FakeBackend))
            .unwrap();
        let config = register.config.clone();
        let host = register.into_start_services().unwrap();
        (
            StartCtx {
                shutdown: host_api::ShutdownToken::new(shutdown_rx),
                paths,
                config,
                host,
            },
            shutdown_tx,
        )
    }

    fn signed_headers(body: &[u8]) -> axum::http::HeaderMap {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-hub-signature-256",
            format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
                .parse()
                .unwrap(),
        );
        headers
    }
    #[test]
    fn adapts_markdown() {
        assert_eq!(adapt_markdown("# **Hi**\n**yes**"), "*Hi*\n*yes*");
    }
    #[test]
    fn rate_limits_statuses() {
        let mut last = HashMap::new();
        let now = Instant::now();
        assert!(status_allowed(&mut last, "1", now));
        assert!(!status_allowed(
            &mut last,
            "1",
            now + Duration::from_secs(1)
        ));
        assert!(status_allowed(&mut last, "1", now + Duration::from_secs(2)));
    }
    #[test]
    fn disabled_tool_status_never_consumes_rate_limit() {
        let mut last = HashMap::new();
        assert!(!tool_status_allowed(false, &mut last, "1", Instant::now()));
        assert!(last.is_empty());
    }
    #[tokio::test]
    async fn typing_keepalive_stops_when_turn_ends() {
        let (stop, receiver) = watch::channel(false);
        let sends = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&sends);
        let task = tokio::spawn(typing_loop(receiver, Duration::from_secs(60), move || {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::Relaxed);
            }
        }));
        while sends.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sends.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn typing_keepalive_cancels_an_inflight_post() {
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(typing_loop(receiver, Duration::from_secs(60), || async {
            std::future::pending::<()>().await;
        }));
        tokio::task::yield_now().await;
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn signed_webhook_roundtrip_replies_once_with_context() {
        let requests = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let captured = Arc::clone(&requests);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/messages",
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let captured = Arc::clone(&captured);
                async move {
                    captured.lock().unwrap().push(body);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let graph = Arc::new(Graph::with_base(format!("http://{address}/messages")).unwrap());
        let (tx, mut rx) = mpsc::channel(2);
        let webhook = WebhookState {
            verify_token: Some("verify".into()),
            app_secret: Some("secret".into()),
            phone_number_id: "phone".into(),
            inbound: tx,
            dedup: Default::default(),
        };
        let payload = br#"{"entry":[{"changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"phone"},"messages":[{"from":"3361","id":"wamid-1","type":"text","text":{"body":"hello"}}]}}]}]}"#;
        assert_eq!(
            webhook::handle_inbound(
                webhook.clone(),
                signed_headers(payload),
                axum::body::Body::from(payload.as_slice())
            )
            .await
            .status(),
            axum::http::StatusCode::OK
        );
        let incoming = rx.recv().await.unwrap();
        assert_eq!(
            webhook::handle_inbound(
                webhook,
                signed_headers(payload),
                axum::body::Body::from(payload.as_slice())
            )
            .await
            .status(),
            axum::http::StatusCode::OK
        );
        assert!(rx.try_recv().is_err());

        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown_tx) = test_ctx(temp.path());
        let cfg = WhatsAppConfig::default();
        let mut state = DispatcherState {
            statuses: HashMap::new(),
            sessions: HashMap::new(),
            status_slots: Arc::new(Semaphore::new(1)),
        };
        process(&ctx, &cfg, &graph, temp.path(), incoming, &mut state).await;
        let sent = requests.lock().unwrap();
        let replies = sent
            .iter()
            .filter(|request| request.get("type").and_then(Value::as_str) == Some("text"))
            .collect::<Vec<_>>();
        assert_eq!(replies.len(), 1);
        assert_eq!(
            *replies[0],
            json!({"messaging_product":"whatsapp","recipient_type":"individual","to":"3361","type":"text","text":{"body":"agent reply","preview_url":true},"context":{"message_id":"wamid-1"}})
        );
        server.abort();
    }
}
