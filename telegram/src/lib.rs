//! Telegram chat channel for agentropy.
//!
//! A background extension that makes the agent reachable over a Telegram bot.
//! It long-polls the Telegram Bot API (`getUpdates`), routes each incoming text
//! message to a `dyn ChatBackend` chat session, streams the assistant reply, and
//! sends it back with `sendMessage`. One persistent session per Telegram chat
//! gives each conversation its own context.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dar_extension_sdk::chat::{ChatBackend, ChatEvent, ChatRole, ChatSession};
use dar_extension_sdk::tools::{
    ToolExecutor, ToolOutcome, ToolRegistryHandle, ToolSpec, TOOL_REGISTRY_SERVICE,
};
use dar_extension_sdk::{ConfigStore, Extension, RegisterCtx, ShutdownToken, StartCtx};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

mod ack;
mod markdown;
use ack::{AckGuard, BotApi};
use markdown::{render_chunks, Chunk, ParseMode};
/// Long-poll timeout (seconds) the Telegram server holds an empty `getUpdates`.
const POLL_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct TelegramConfig {
    /// Bot token from BotFather. Falls back to the `TELEGRAM_BOT_TOKEN` env var.
    bot_token: Option<String>,
    /// Whitelist of Telegram user ids; empty means allow everyone.
    allowed_users: Vec<i64>,
    /// Chat backend service id to drive; defaults to the stock "pi" backend.
    backend: Option<String>,
}

pub struct TelegramExtension;

pub fn extension() -> Box<dyn Extension> {
    Box::new(TelegramExtension)
}

impl Extension for TelegramExtension {
    fn id(&self) -> &'static str {
        "telegram"
    }

    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config, self.id())?;
            if resolve_token(&cfg).is_none() {
                bail!(
                    "telegram.bot_token is required: set extensions.telegram.bot_token in \
                     agent.yaml or the TELEGRAM_BOT_TOKEN environment variable"
                );
            }
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn ToolRegistryHandle>(TOOL_REGISTRY_SERVICE)
            {
                let token = resolve_token(&cfg).expect("token checked above");
                registry.register_tool(
                    telegram_send_spec(),
                    Arc::new(TelegramSendTool::new(token)?),
                )?;
            }
            Ok(())
        })
    }

    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config, self.id())?;
            let token = resolve_token(&cfg).context("telegram bot token missing at start")?;

            std::fs::create_dir_all(ctx.paths.root().join("data"))?;
            let session_dir = ctx.paths.data_dir(self.id())?.join("sessions");
            std::fs::create_dir_all(&session_dir)?;

            let mut shutdown = ctx.shutdown.clone();
            tokio::spawn(async move {
                if let Err(err) = run(&ctx, &mut shutdown, &cfg, &token, &session_dir).await {
                    tracing::error!(error = %err, "telegram channel stopped");
                }
            });
            Ok(())
        })
    }
}

fn telegram_send_spec() -> ToolSpec {
    ToolSpec::new(
        "telegram_send_message",
        "Send a Telegram message to an exact chat id through the configured bot.",
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "chat_id": {
                    "type": "integer",
                    "description": "Exact Telegram chat id to send to."
                },
                "text": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Message text to send."
                }
            },
            "required": ["chat_id", "text"]
        }),
    )
    .writes()
}

struct TelegramSendTool {
    client: reqwest::Client,
    base: String,
}

impl TelegramSendTool {
    fn new(token: String) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            base: format!("https://api.telegram.org/bot{token}"),
        })
    }
}

#[async_trait]
impl ToolExecutor for TelegramSendTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        let chat_id = match args.get("chat_id").and_then(Value::as_i64) {
            Some(chat_id) => chat_id,
            None => {
                return Ok(ToolOutcome::error_code(
                    "invalid_args",
                    "telegram_send_message requires integer 'chat_id'",
                    None::<String>,
                ))
            }
        };
        let Some(text) = args.get("text").and_then(Value::as_str).map(str::trim) else {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "telegram_send_message requires non-empty string 'text'",
                None::<String>,
            ));
        };
        if text.is_empty() {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "telegram_send_message requires non-empty string 'text'",
                None::<String>,
            ));
        }

        if let Err(err) = send_reply(&self.client, &self.base, chat_id, text).await {
            return Ok(ToolOutcome::error_code(
                "send_failed",
                format!("Telegram sendMessage failed: {err:#}"),
                None::<String>,
            ));
        }
        Ok(ToolOutcome::ok(format!(
            "sent Telegram message to chat {chat_id}"
        )))
    }
}

fn parse_config(config: &ConfigStore, id: &str) -> Result<TelegramConfig> {
    match config.get(id) {
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(TelegramConfig::default()),
    }
}

fn resolve_token(cfg: &TelegramConfig) -> Option<String> {
    cfg.bot_token.clone().filter(|t| !t.is_empty()).or_else(|| {
        std::env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
    })
}

fn authorized(user_id: Option<i64>, allowed: &[i64]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    matches!(user_id, Some(id) if allowed.contains(&id))
}

/// Send an agent reply through the fallback chain that guarantees delivery:
/// MarkdownV2 → plain text. Each chunk is tried as rendered MarkdownV2 first;
/// if Telegram rejects it with a parse error (malformed markup), that chunk's
/// *exact source span* is re-sent as plain text so the content is never lost.
///
/// Because `render_chunks` chunks the raw source (not the escaped output), a
/// chunk's `source` covers the same bytes as its rich rendering — so the
/// plain-text fallback is lossless and index-aligned by construction. Non-parse
/// network errors propagate so the caller can log a genuine delivery failure.
async fn send_reply(client: &reqwest::Client, base: &str, chat_id: i64, reply: &str) -> Result<()> {
    for chunk in render_chunks(reply) {
        match send_chunk(client, base, chat_id, &chunk).await {
            Ok(()) => {}
            Err(SendError::Parse) => {
                // Malformed markup: fall to a lossless plain-text send of the
                // same source span.
                let fallback = Chunk {
                    text: chunk.source.clone(),
                    parse_mode: ParseMode::Plain,
                    source: chunk.source,
                };
                send_chunk(client, base, chat_id, &fallback)
                    .await
                    .map_err(SendError::into_anyhow)?;
            }
            Err(SendError::Other(err)) => return Err(err),
        }
    }
    Ok(())
}

/// Outcome of a single `sendMessage`: a Telegram parse error (which triggers
/// the plain-text fallback) is distinguished from any other failure.
enum SendError {
    /// Telegram rejected the message because its markup could not be parsed.
    Parse,
    /// A transport or non-parse API error.
    Other(anyhow::Error),
}

impl SendError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            SendError::Parse => anyhow::anyhow!("telegram parse error"),
            SendError::Other(err) => err,
        }
    }
}

/// Send one already-chunked message with its chosen parse mode.
async fn send_chunk(
    client: &reqwest::Client,
    base: &str,
    chat_id: i64,
    chunk: &Chunk,
) -> std::result::Result<(), SendError> {
    let mut payload = serde_json::json!({ "chat_id": chat_id, "text": chunk.text });
    if let Some(mode) = chunk.parse_mode.as_api_value() {
        payload["parse_mode"] = serde_json::Value::String(mode.to_string());
    }
    let resp = client
        .post(format!("{base}/sendMessage"))
        .json(&payload)
        .send()
        .await
        .map_err(|e| SendError::Other(e.into()))?;
    let status = resp.status();
    let body: TgResponse<Value> = resp.json().await.map_err(|e| SendError::Other(e.into()))?;
    if body.ok {
        return Ok(());
    }
    let desc = body.description.unwrap_or_default();
    if is_parse_error(&desc) {
        return Err(SendError::Parse);
    }
    Err(SendError::Other(anyhow::anyhow!(
        "telegram sendMessage failed ({status}): {desc}"
    )))
}

/// Does Telegram's error description indicate a MarkdownV2 parse failure?
fn is_parse_error(description: &str) -> bool {
    let d = description.to_ascii_lowercase();
    d.contains("can't parse entities") || d.contains("can't parse message text")
}

/// One live agent conversation, keyed by Telegram chat id.
struct ChatConn {
    session: Box<dyn ChatSession>,
    rx: mpsc::Receiver<ChatEvent>,
}

async fn run(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    cfg: &TelegramConfig,
    token: &str,
    session_dir: &Path,
) -> Result<()> {
    let base = format!("https://api.telegram.org/bot{token}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(POLL_TIMEOUT_SECS + 5))
        .build()?;
    let bot_api: Arc<dyn BotApi> = Arc::new(TelegramBotApi {
        client: client.clone(),
        base: base.clone(),
    });
    let mut offset: i64 = 0;
    let mut sessions: HashMap<i64, ChatConn> = HashMap::new();

    dar_extension_sdk::log::event(
        "-",
        "telegram",
        "extension enabled; connecting to Telegram bot API",
    );

    match get_me(&client, &base).await {
        Ok(username) => {
            dar_extension_sdk::log::event("-", "telegram", &format!("connected as @{username}"))
        }
        Err(err) => tracing::warn!(error = %err, "telegram getMe failed; continuing to poll"),
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = poll_updates(&client, &base, offset) => {
                let updates = match result {
                    Ok(updates) => updates,
                    Err(err) => {
                        tracing::warn!(error = %err, "telegram getUpdates failed; backing off");
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                        }
                        continue;
                    }
                };
                for update in updates {
                    offset = update.update_id + 1;
                    let Some(message) = update.message else { continue };
                    let message_id = message.message_id;
                    let Some(text) = message.text else { continue };
                    let chat_id = message.chat.id;
                    let user_id = message.from.map(|u| u.id);
                    if !authorized(user_id, &cfg.allowed_users) {
                        let _ = send_message(&client, &base, chat_id, "Not authorized.").await;
                        continue;
                    }
                    dar_extension_sdk::log::event(
                        "-",
                        "telegram",
                        &format!(
                            "message from chat {} (user {})",
                            chat_id,
                            user_id.map(|u| u.to_string()).unwrap_or_else(|| "?".into()),
                        ),
                    );
                    // Acknowledge the moment the message is picked up: the guard
                    // adds the 👀 reaction + keeps typing alive, and guarantees
                    // both clear on drop regardless of how the turn ends.
                    let guard =
                        AckGuard::start(Arc::clone(&bot_api), chat_id, message_id).await;
                    let reply = run_turn(
                        ctx,
                        shutdown,
                        &mut sessions,
                        session_dir,
                        cfg.backend.as_deref(),
                        chat_id,
                        text,
                    )
                    .await;
                    if let Err(err) = send_reply(&client, &base, chat_id, &reply).await {
                        tracing::warn!(error = %err, "telegram sendMessage failed");
                    }
                    // Reply delivered: clear 👀 and stop typing, awaiting the
                    // clear so it lands before the next message is picked up.
                    // (Error/panic paths fall back to the guard's Drop.)
                    guard.finish().await;
                }
            }
        }
    }
    Ok(())
}

async fn run_turn(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    sessions: &mut HashMap<i64, ChatConn>,
    base_dir: &Path,
    configured: Option<&str>,
    chat_id: i64,
    text: String,
) -> String {
    if let std::collections::hash_map::Entry::Vacant(slot) = sessions.entry(chat_id) {
        let dir = base_dir.join(chat_id.to_string());
        if let Err(err) = std::fs::create_dir_all(&dir) {
            return format!("Failed to create session dir: {err}");
        }
        match open_session(ctx, &dir, configured).await {
            Ok(conn) => {
                slot.insert(conn);
            }
            Err(err) => return format!("Failed to start agent session: {err}"),
        }
    }

    let conn = sessions.get_mut(&chat_id).expect("session just inserted");
    if let Err(err) = conn.session.send_turn(text).await {
        sessions.remove(&chat_id);
        return format!("Failed to send message: {err}");
    }

    let mut reply = String::new();
    let mut drop_session = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            event = conn.rx.recv() => match event {
                Some(ChatEvent::Delta { role: ChatRole::Assistant, text }) => reply.push_str(&text),
                Some(ChatEvent::TurnFinished { ok: true, .. }) => break,
                Some(ChatEvent::TurnFinished { ok: false, error }) => {
                    if reply.is_empty() {
                        reply = format!("(turn failed: {})", error.unwrap_or_else(|| "unknown".into()));
                    }
                    break;
                }
                Some(ChatEvent::SessionClosed { error }) => {
                    drop_session = true;
                    if reply.is_empty() {
                        reply = format!("(session closed: {})", error.unwrap_or_default());
                    }
                    break;
                }
                Some(_) => {}
                None => {
                    drop_session = true;
                    break;
                }
            }
        }
    }

    if drop_session {
        sessions.remove(&chat_id);
    }
    if reply.trim().is_empty() {
        "(no response)".to_string()
    } else {
        reply
    }
}

async fn open_session(
    ctx: &StartCtx,
    session_dir: &Path,
    configured: Option<&str>,
) -> Result<ChatConn> {
    // Resolve the backend and build params through the shared SDK helpers so
    // Telegram talks to the same agent identity as TUI chat: model/provider
    // from the retained RunSnapshot, the retained `system.context` as the
    // system prompt, and the host tool bridge. Keeping this on the SDK path
    // means a new chat surface inherits the agent's system prompt by default
    // instead of having to remember to copy it.
    let backend_id = dar_extension_sdk::chat::resolve_agent_backend(ctx, configured);
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;

    let params = dar_extension_sdk::chat::agent_session_params(ctx, session_dir).build();

    let (tx, rx) = mpsc::channel(256);
    let session = backend.open(params, tx).await?;
    Ok(ChatConn { session, rx })
}

#[derive(Deserialize)]
struct TgResponse<T> {
    ok: bool,
    #[serde(default)]
    result: Option<T>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Deserialize)]
struct Update {
    update_id: i64,
    #[serde(default)]
    message: Option<TgMessage>,
}

#[derive(Deserialize)]
struct TgMessage {
    message_id: i64,
    #[serde(default)]
    text: Option<String>,
    chat: TgChat,
    #[serde(default)]
    from: Option<TgUser>,
}

#[derive(Deserialize)]
struct TgChat {
    id: i64,
}

#[derive(Default, Deserialize)]
struct TgUser {
    id: i64,
    #[serde(default)]
    username: Option<String>,
}

/// Confirm the bot token by calling `getMe`; returns the bot's username.
async fn get_me(client: &reqwest::Client, base: &str) -> Result<String> {
    let body: TgResponse<TgUser> = client
        .post(format!("{base}/getMe"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if !body.ok {
        bail!(
            "telegram getMe error: {}",
            body.description.unwrap_or_default()
        );
    }
    Ok(body.result.and_then(|u| u.username).unwrap_or_default())
}

async fn poll_updates(client: &reqwest::Client, base: &str, offset: i64) -> Result<Vec<Update>> {
    let body: TgResponse<Vec<Update>> = client
        .post(format!("{base}/getUpdates"))
        .json(&serde_json::json!({
            "offset": offset,
            "timeout": POLL_TIMEOUT_SECS,
            "allowed_updates": ["message"],
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if !body.ok {
        bail!(
            "telegram getUpdates error: {}",
            body.description.unwrap_or_default()
        );
    }
    Ok(body.result.unwrap_or_default())
}

async fn send_message(
    client: &reqwest::Client,
    base: &str,
    chat_id: i64,
    text: &str,
) -> Result<()> {
    client
        .post(format!("{base}/sendMessage"))
        .json(&serde_json::json!({ "chat_id": chat_id, "text": text }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Set or clear a message reaction. `emoji = None` clears all reactions.
async fn set_message_reaction(
    client: &reqwest::Client,
    base: &str,
    chat_id: i64,
    message_id: i64,
    emoji: Option<&str>,
) -> Result<()> {
    let reaction = match emoji {
        Some(emoji) => serde_json::json!([{ "type": "emoji", "emoji": emoji }]),
        None => serde_json::json!([]),
    };
    client
        .post(format!("{base}/setMessageReaction"))
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "reaction": reaction,
        }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Send a chat action such as `typing` to show the activity indicator.
async fn send_chat_action(
    client: &reqwest::Client,
    base: &str,
    chat_id: i64,
    action: &str,
) -> Result<()> {
    client
        .post(format!("{base}/sendChatAction"))
        .json(&serde_json::json!({ "chat_id": chat_id, "action": action }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Live Bot API sink for the acknowledgement guard, backed by the shared
/// `reqwest` client. Network errors are logged and swallowed: the guard is
/// best-effort feedback and must never break delivery of the real reply.
struct TelegramBotApi {
    client: reqwest::Client,
    base: String,
}

#[async_trait]
impl BotApi for TelegramBotApi {
    async fn set_reaction(&self, chat_id: i64, message_id: i64, emoji: Option<&str>) {
        if let Err(err) =
            set_message_reaction(&self.client, &self.base, chat_id, message_id, emoji).await
        {
            tracing::warn!(error = %err, "telegram setMessageReaction failed");
        }
    }

    async fn send_chat_action(&self, chat_id: i64, action: &str) {
        if let Err(err) = send_chat_action(&self.client, &self.base, chat_id, action).await {
            tracing::warn!(error = %err, "telegram sendChatAction failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allowlist_permits_anyone() {
        assert!(authorized(Some(5), &[]));
        assert!(authorized(None, &[]));
    }

    #[test]
    fn allowlist_gates_by_user_id() {
        assert!(authorized(Some(5), &[5, 9]));
        assert!(!authorized(Some(7), &[5, 9]));
        assert!(!authorized(None, &[5]));
    }

    #[test]
    fn is_parse_error_detects_telegram_markup_failures() {
        assert!(is_parse_error(
            "Bad Request: can't parse entities: unmatched '*'"
        ));
        assert!(is_parse_error("Bad Request: can't parse message text"));
        assert!(!is_parse_error("Bad Request: chat not found"));
    }

    #[test]
    fn telegram_send_spec_requires_exact_chat_id_and_text() {
        let spec = telegram_send_spec();
        assert_eq!(spec.name, "telegram_send_message");
        assert_eq!(spec.input_schema["required"], json!(["chat_id", "text"]));
        assert_eq!(
            spec.input_schema["properties"]["chat_id"]["type"],
            "integer"
        );
    }

    #[tokio::test]
    async fn telegram_send_rejects_empty_text() {
        let tool = TelegramSendTool::new("test-token".to_string()).unwrap();
        let out = tool
            .execute(json!({ "chat_id": 123_i64, "text": "   " }))
            .await
            .unwrap();
        assert!(out.is_error);
        assert_eq!(out.error.as_ref().unwrap().code, "invalid_args");
    }
}
