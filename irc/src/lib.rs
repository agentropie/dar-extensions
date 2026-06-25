//! IRC channel extension for agentropy.
//!
//! A background extension that makes the agent reachable over IRC — primarily in
//! shared team channels holding both humans and multiple agents, and over private
//! DMs. It connects over TCP/TLS, joins configured channels, replies only when
//! addressed by nick (always answers DMs), ingests surrounding channel traffic as
//! bounded ambient context, and enforces a hard cap on consecutive bot-to-bot
//! turns so an exchange can never spiral token cost with no human present.
//!
//! It mirrors the telegram extension's shape: a background [`dar_extension_sdk::Extension`]
//! driving the host's `cap-chat` `ChatBackend`, resolved from the stock "pi" backend
//! composed via requires_stock, wiring the host-tool bridge, with one session per
//! conversation.

mod addressing;
mod config;
mod conn;
mod loop_guard;
mod proto;
mod split;

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dar_extension_sdk::chat::{ChatBackend, ChatEvent, ChatRole, ChatSession, ChatSessionParams};
use dar_extension_sdk::orchestrator::{RunSnapshot, RUN_SNAPSHOT_TOPIC};
use dar_extension_sdk::tools::{
    ToolExecutor, ToolOutcome, ToolRegistryHandle, ToolSpec, TOOL_REGISTRY_SERVICE,
};
use dar_extension_sdk::{ConfigStore, Extension, RegisterCtx, ShutdownToken, StartCtx};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use addressing::{classify, is_channel, strip_mention, Conversation, Verdict};
use config::{dm_authorized, IrcConfig};
use conn::{connect_and_register, Connection, SEND_PACING};
use loop_guard::LoopGuard;
use proto::PrivMsg;

/// Backend id used as the default fallback: the stock "pi" backend composed in
/// via requires_stock = ["chat-pi"] in [package.metadata.dar].
const DEFAULT_BACKEND_ID: &str = "pi";
/// Reconnect backoff bounds.
const BACKOFF_MIN: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Brief window for a one-shot outbound tool call to observe immediate IRC
/// rejection numerics after writing PRIVMSG.
const OUTBOUND_ERROR_WAIT: Duration = Duration::from_millis(750);

pub struct IrcExtension;

pub fn extension() -> Box<dyn Extension> {
    Box::new(IrcExtension)
}

impl Extension for IrcExtension {
    fn id(&self) -> &'static str {
        "irc"
    }

    fn register<'a>(&'a self, ctx: &'a mut RegisterCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config, self.id())?;
            if cfg.server.is_none() {
                bail!(
                    "irc.server is required: set extensions.irc.server in agent.yaml or the \
                     IRC_SERVER environment variable"
                );
            }
            if cfg.nick.is_none() {
                bail!(
                    "irc.nick is required: set extensions.irc.nick in agent.yaml or the \
                     IRC_NICK environment variable"
                );
            }
            if let Ok(registry) = ctx
                .services
                .get_named::<dyn ToolRegistryHandle>(TOOL_REGISTRY_SERVICE)
            {
                registry.register_tool(
                    irc_send_spec(),
                    Arc::new(IrcSendTool { cfg: cfg.clone() }),
                )?;
            }
            Ok(())
        })
    }

    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse_config(&ctx.config, self.id())?;

            std::fs::create_dir_all(ctx.paths.root().join("data"))?;
            let session_dir = ctx.paths.data_dir(self.id())?.join("sessions");
            std::fs::create_dir_all(&session_dir)?;

            let mut shutdown = ctx.shutdown.clone();
            tokio::spawn(async move {
                if let Err(err) = run(&ctx, &mut shutdown, &cfg, &session_dir).await {
                    tracing::error!(error = %err, "irc channel stopped");
                }
            });
            Ok(())
        })
    }
}

fn irc_send_spec() -> ToolSpec {
    ToolSpec::new(
        "irc_send_message",
        "Send an IRC PRIVMSG to an exact channel or nick through the live IRC connection.",
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "target": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Exact IRC channel or nick to send to."
                },
                "text": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Message text to send."
                }
            },
            "required": ["target", "text"]
        }),
    )
    .writes()
}

struct IrcSendTool {
    cfg: IrcConfig,
}

#[derive(Deserialize)]
struct IrcSendArgs {
    target: String,
    text: String,
}

#[async_trait]
impl ToolExecutor for IrcSendTool {
    async fn execute(&self, args: Value) -> Result<ToolOutcome> {
        let args: IrcSendArgs = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(err) => {
                return Ok(ToolOutcome::error_code(
                    "invalid_args",
                    format!("invalid irc_send_message arguments: {err}"),
                    None::<String>,
                ));
            }
        };
        let target = args.target.trim();
        let text = args.text.trim();
        if target.is_empty() || text.is_empty() {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "irc_send_message requires non-empty 'target' and 'text'",
                None::<String>,
            ));
        }
        if !valid_irc_target(target) {
            return Ok(ToolOutcome::error_code(
                "invalid_args",
                "irc_send_message target must be a single IRC channel or nick without whitespace or line breaks",
                None::<String>,
            ));
        }
        let mut conn = match connect_and_register(&self.cfg).await {
            Ok(conn) => conn,
            Err(err) => {
                return Ok(ToolOutcome::error_code(
                    "connect_failed",
                    format!("IRC connect/register failed: {err:#}"),
                    None::<String>,
                ));
            }
        };
        if let Err(err) = send_reply(&conn.sender(), target, text).await {
            return Ok(ToolOutcome::error_code(
                "send_failed",
                format!("IRC PRIVMSG failed: {err:#}"),
                None::<String>,
            ));
        }
        if let Err(err) = observe_privmsg_rejection(&mut conn, target).await {
            return Ok(ToolOutcome::error_code(
                "send_failed",
                format!("IRC server rejected PRIVMSG: {err:#}"),
                None::<String>,
            ));
        }
        Ok(ToolOutcome::ok(format!("sent IRC message to {target}")))
    }
}

fn valid_irc_target(target: &str) -> bool {
    !target.is_empty()
        && !target
            .chars()
            .any(|ch| ch == '\r' || ch == '\n' || ch.is_whitespace())
}

fn parse_config(config: &ConfigStore, id: &str) -> Result<IrcConfig> {
    let cfg = match config.get(id) {
        Some(value) => serde_json::from_value(value.clone())?,
        None => IrcConfig::default(),
    };
    Ok(cfg.with_env_fallbacks())
}

/// One live agent conversation, keyed by [`Conversation`].
struct ChatConn {
    session: Box<dyn ChatSession>,
    rx: mpsc::Receiver<ChatEvent>,
    /// Bounded ring of recent ambient (context-only) messages awaiting flush
    /// into the next turn's prompt.
    ambient: VecDeque<String>,
}

struct ChannelState {
    sessions: HashMap<Conversation, ChatConn>,
    guard: LoopGuard,
}

async fn run(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    cfg: &IrcConfig,
    session_dir: &Path,
) -> Result<()> {
    let mut backoff = BACKOFF_MIN;
    let mut state = ChannelState {
        sessions: HashMap::new(),
        guard: LoopGuard::new(cfg.effective_max_bot_turns()),
    };

    dar_extension_sdk::log::event(
        "-",
        "irc",
        &format!(
            "extension enabled; connecting to {}:{} (tls={}) as {}",
            cfg.server.as_deref().unwrap_or(""),
            cfg.effective_port(),
            cfg.tls(),
            cfg.nick.as_deref().unwrap_or(""),
        ),
    );

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let conn = tokio::select! {
            _ = shutdown.cancelled() => break,
            result = connect_and_register(cfg) => match result {
                Ok(conn) => conn,
                Err(err) => {
                    tracing::warn!(error = %err, ?backoff, "irc connect failed; backing off");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    continue;
                }
            }
        };
        // Connected: reset backoff and serve until the link drops.
        backoff = BACKOFF_MIN;
        dar_extension_sdk::log::event(
            "-",
            "irc",
            &format!(
                "connected to {}; joined {}",
                cfg.server.as_deref().unwrap_or(""),
                cfg.channel_names().collect::<Vec<_>>().join(" "),
            ),
        );
        match serve(ctx, shutdown, cfg, session_dir, &mut state, conn).await {
            Ok(true) => break, // graceful shutdown
            Ok(false) => {
                tracing::warn!("irc connection closed; reconnecting");
            }
            Err(err) => {
                tracing::warn!(error = %err, "irc connection error; reconnecting");
            }
        }
        // Drop per-connection sessions; conversation context is persisted on disk.
        state.sessions.clear();
    }
    Ok(())
}

/// Serve one live connection. Returns `Ok(true)` on graceful shutdown, `Ok(false)`
/// on a clean disconnect (reconnect), or `Err` on a read error (reconnect).
async fn serve(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    cfg: &IrcConfig,
    session_dir: &Path,
    state: &mut ChannelState,
    mut conn: Connection,
) -> Result<bool> {
    let sender = conn.sender();

    loop {
        let msg = tokio::select! {
            _ = shutdown.cancelled() => return Ok(true),
            next = conn.next_message() => match next? {
                Some(m) => m,
                None => return Ok(false),
            }
        };

        // Read the live nick fresh each turn: a server-initiated NICK change
        // (NickServ enforcement, SANICK, ghost/regain) updates it inside
        // next_message, so self-ignore stays correct after a forced rename.
        let bot_nick = conn.nick.clone();

        let Some(pm) = PrivMsg::from_message(&msg) else {
            continue;
        };
        let mention_required = if is_channel(&pm.target) {
            cfg.mention_required_for(&pm.target)
        } else {
            true
        };
        let (verdict, conv) = classify(&pm, &bot_nick, mention_required);

        match verdict {
            Verdict::Ignore => continue,
            Verdict::ContextOnly => {
                // Ambient channel traffic: a human resets the loop-guard; either
                // way the line is buffered as bounded context for the next turn.
                if let Conversation::Channel(ch) = &conv {
                    if !sender_is_bot(&pm.sender, &cfg.humans) {
                        state.guard.note_human(ch);
                    }
                }
                buffer_ambient(
                    state,
                    &conv,
                    &pm,
                    cfg.effective_context_window(),
                    ctx,
                    session_dir,
                    cfg,
                )
                .await;
            }
            Verdict::Reply => {
                if let Conversation::Dm(nick) = &conv {
                    if !dm_authorized(nick, &cfg.allowed_users) {
                        tracing::info!(nick, "irc DM from non-allowlisted nick; ignoring");
                        continue;
                    }
                }
                // Loop-guard: cap consecutive bot-to-bot turns. Applies to BOTH
                // channels AND DMs — a DM is an unattended channel, so two agents
                // DMing each other must hit the same hard cap (the non-negotiable
                // guarantee: no runaway bot-to-bot cost with no human present).
                let is_bot = sender_is_bot(&pm.sender, &cfg.humans);
                let guard_key = conv.guard_key();
                if !state.guard.should_respond(is_bot, &guard_key) {
                    tracing::info!(
                        conversation = %guard_key,
                        consecutive_bot_turns = state.guard.count(&guard_key),
                        "irc bot-to-bot cap reached; staying silent"
                    );
                    // Still ingest as context so the bot stays aware.
                    buffer_ambient(
                        state,
                        &conv,
                        &pm,
                        cfg.effective_context_window(),
                        ctx,
                        session_dir,
                        cfg,
                    )
                    .await;
                    continue;
                }

                dar_extension_sdk::log::event(
                    "-",
                    "irc",
                    &format!("message from {} in {}", pm.sender, guard_key),
                );
                let prompt = build_prompt(state, &conv, &pm, &bot_nick);
                let reply = run_turn(
                    ctx,
                    shutdown,
                    state,
                    session_dir,
                    cfg.backend.as_deref(),
                    &conv,
                    prompt,
                )
                .await;
                let target = conv.reply_target(&pm.sender);
                if let Err(err) = send_reply(&sender, &target, &reply).await {
                    tracing::warn!(error = %err, target, "irc PRIVMSG failed");
                }
            }
        }
    }
}

/// A channel sender is treated as a bot for loop-guard purposes unless it is a
/// known human (on the `humans` list, case-insensitive). This is deliberately
/// fail-closed: with an empty `humans` list we cannot positively identify a
/// human, so every sender counts toward the consecutive-bot-turn cap. That keeps
/// the non-negotiable guarantee intact — the cap can never be silently disabled,
/// and two agents addressing each other always hit it. Operators who want
/// uncapped human-driven exchanges list their humans explicitly. This is
/// independent of `allowed_users`, which is purely a DM authorization gate.
fn sender_is_bot(sender: &str, humans: &[String]) -> bool {
    !humans.iter().any(|h| h.eq_ignore_ascii_case(sender))
}

/// Buffer an ambient message into the conversation's context ring (bounded to
/// `window`), opening the session lazily so context accrues even before the first
/// reply.
async fn buffer_ambient(
    state: &mut ChannelState,
    conv: &Conversation,
    pm: &PrivMsg,
    window: usize,
    ctx: &StartCtx,
    session_dir: &Path,
    cfg: &IrcConfig,
) {
    if window == 0 {
        return;
    }
    if ensure_session(ctx, state, session_dir, cfg.backend.as_deref(), conv)
        .await
        .is_err()
    {
        return;
    }
    let conn = state.sessions.get_mut(conv).expect("session ensured");
    conn.ambient
        .push_back(format!("<{}> {}", pm.sender, pm.text));
    while conn.ambient.len() > window {
        conn.ambient.pop_front();
    }
}

/// Compose the turn prompt: any buffered ambient context (drained) followed by
/// the addressed request (mention prefix stripped).
fn build_prompt(
    state: &mut ChannelState,
    conv: &Conversation,
    pm: &PrivMsg,
    bot_nick: &str,
) -> String {
    let request = strip_mention(&pm.text, bot_nick);
    let ambient = state
        .sessions
        .get_mut(conv)
        .map(|c| std::mem::take(&mut c.ambient))
        .unwrap_or_default();
    if ambient.is_empty() {
        format!("<{}> {}", pm.sender, request)
    } else {
        format!(
            "Recent channel context:\n{}\n\nNow {} addresses you:\n{}",
            ambient.iter().cloned().collect::<Vec<_>>().join("\n"),
            pm.sender,
            request
        )
    }
}

async fn send_reply(sender: &conn::Sender, target: &str, reply: &str) -> Result<()> {
    let mut first = true;
    for chunk in split::split_message(reply, target) {
        if !first {
            tokio::time::sleep(SEND_PACING).await;
        }
        first = false;
        sender.privmsg(target, &chunk).await?;
    }
    Ok(())
}

async fn observe_privmsg_rejection(conn: &mut Connection, target: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + OUTBOUND_ERROR_WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        let next = tokio::time::timeout(remaining, conn.next_message()).await;
        let msg = match next {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) | Err(_) => return Ok(()),
            Ok(Err(err)) => return Err(err),
        };
        if is_privmsg_rejection(&msg.command) && message_mentions_target(&msg.params, target) {
            bail!("{}", msg.params.join(" "));
        }
    }
}

fn is_privmsg_rejection(command: &str) -> bool {
    matches!(
        command,
        "401" | "403"
            | "404"
            | "405"
            | "407"
            | "408"
            | "411"
            | "412"
            | "413"
            | "414"
            | "442"
            | "482"
    )
}

fn message_mentions_target(params: &[String], target: &str) -> bool {
    params.iter().any(|param| param.eq_ignore_ascii_case(target))
}

/// Ensure a session exists for `conv`, creating it (and its on-disk dir) lazily.
async fn ensure_session(
    ctx: &StartCtx,
    state: &mut ChannelState,
    base_dir: &Path,
    configured: Option<&str>,
    conv: &Conversation,
) -> Result<()> {
    if state.sessions.contains_key(conv) {
        return Ok(());
    }
    let dir = base_dir.join(conv.key());
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating session dir {}", dir.display()))?;
    let connection = open_session(ctx, &dir, configured).await?;
    state.sessions.insert(conv.clone(), connection);
    Ok(())
}

async fn run_turn(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    state: &mut ChannelState,
    base_dir: &Path,
    configured: Option<&str>,
    conv: &Conversation,
    text: String,
) -> String {
    if let Err(err) = ensure_session(ctx, state, base_dir, configured, conv).await {
        return format!("Failed to start agent session: {err}");
    }

    let conn = state.sessions.get_mut(conv).expect("session just ensured");
    if let Err(err) = conn.session.send_turn(text).await {
        state.sessions.remove(conv);
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
        state.sessions.remove(conv);
    }
    if reply.trim().is_empty() {
        "(no response)".to_string()
    } else {
        reply
    }
}

/// Pick the chat backend id, mirroring the TUI: an explicit, *registered* config
/// override wins; else follow the orchestrator's selected runner when it is
/// registered as a chat backend; else fall back to the stock "pi" backend
/// (`DEFAULT_BACKEND_ID`), composed in via requires_stock = ["chat-pi"].
fn resolve_backend(configured: Option<&str>, ctx: &StartCtx) -> String {
    let registered = |id: &str| ctx.host.services.get::<dyn ChatBackend>(id).is_ok();
    if let Some(id) = configured {
        if !id.is_empty() && registered(id) {
            return id.to_string();
        }
    }
    let runner = ctx
        .host
        .bus
        .read_retained::<RunSnapshot>(RUN_SNAPSHOT_TOPIC)
        .ok()
        .filter(|s| s.version > 0)
        .map(|s| s.agent.runner)
        .filter(|r| !r.is_empty());
    if let Some(runner) = runner {
        if registered(&runner) {
            return runner;
        }
    }
    DEFAULT_BACKEND_ID.to_string()
}

async fn open_session(
    ctx: &StartCtx,
    session_dir: &Path,
    configured: Option<&str>,
) -> Result<ChatConn> {
    let backend_id = resolve_backend(configured, ctx);
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;

    let snapshot = ctx
        .host
        .bus
        .read_retained::<RunSnapshot>(RUN_SNAPSHOT_TOPIC)
        .ok()
        .filter(|s| s.version > 0);
    let model = snapshot.as_ref().and_then(|s| s.agent.model.clone());
    let provider = snapshot.as_ref().and_then(|s| s.agent.provider.clone());

    let params = ChatSessionParams::builder("", ctx.paths.root(), session_dir)
        .model(model)
        .provider(provider)
        .host_tool_bridge(dar_extension_sdk::tools::host_tool_bridge(
            &ctx.host.services,
            ctx.paths.root(),
        ))
        .build();

    let (tx, rx) = mpsc::channel(256);
    let session = backend.open(params, tx).await?;
    Ok(ChatConn {
        session,
        rx,
        ambient: VecDeque::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_is_bot_with_empty_humans_treats_all_as_bots() {
        // Fail-closed: with no known humans, every sender counts toward the cap.
        assert!(sender_is_bot("anyone", &[]));
    }

    #[test]
    fn irc_send_spec_requires_exact_target_and_text() {
        let spec = irc_send_spec();
        assert_eq!(spec.name, "irc_send_message");
        assert_eq!(spec.input_schema["required"], json!(["target", "text"]));
        assert_eq!(spec.input_schema["properties"]["target"]["type"], "string");
    }

    #[test]
    fn irc_send_rejects_line_injection_targets() {
        assert!(valid_irc_target("#team"));
        assert!(valid_irc_target("alice"));
        assert!(!valid_irc_target("#team\r\nJOIN #other"));
        assert!(!valid_irc_target("#team other"));
        assert!(!valid_irc_target(""));
    }

    #[tokio::test]
    async fn irc_send_reports_connect_failure() {
        let tool = IrcSendTool {
            cfg: IrcConfig {
                server: Some("127.0.0.1".to_string()),
                port: Some(1),
                nick: Some("darbot".to_string()),
                tls: Some(false),
                ..IrcConfig::default()
            },
        };
        let out = tool
            .execute(json!({ "target": "#team", "text": "hello" }))
            .await
            .unwrap();
        assert!(out.is_error);
        assert_eq!(out.error.as_ref().unwrap().code, "connect_failed");
    }

    #[test]
    fn sender_is_bot_recognizes_listed_humans() {
        let humans = vec!["alice".to_string(), "Bob".to_string()];
        assert!(!sender_is_bot("alice", &humans));
        assert!(!sender_is_bot("BOB", &humans)); // case-insensitive
        assert!(sender_is_bot("otheragent", &humans));
    }

    /// End-to-end wiring of the loop-guard non-negotiable: drive a realistic
    /// bot-to-bot exchange through classify() -> sender_is_bot() -> should_respond()
    /// with the DEFAULT (empty) config and assert the hard cap actually fires.
    /// This is the production path that the loop_guard unit tests bypass.
    #[test]
    fn bot_to_bot_cap_fires_end_to_end_with_default_config() {
        let cfg = IrcConfig::default(); // empty allowed_users AND empty humans
        let bot_nick = "darbot";
        let mut guard = LoopGuard::new(cfg.effective_max_bot_turns());

        // Another agent repeatedly addresses our bot in a channel.
        let mut allowed_replies = 0u32;
        for _ in 0..(cfg.effective_max_bot_turns() + 10) {
            let pm = PrivMsg {
                sender: "otheragent".into(),
                target: "#agents".into(),
                text: format!("{bot_nick}: your turn"),
            };
            let (verdict, conv) = classify(&pm, bot_nick, true);
            assert_eq!(verdict, Verdict::Reply);
            let Conversation::Channel(ch) = &conv else {
                panic!("expected channel conversation");
            };
            let is_bot = sender_is_bot(&pm.sender, &cfg.humans);
            assert!(is_bot, "unknown sender must be treated as a bot");
            if guard.should_respond(is_bot, ch) {
                allowed_replies += 1;
            }
        }
        // The cap must engage: only max_bot_turns replies, never unbounded.
        assert_eq!(allowed_replies, cfg.effective_max_bot_turns());
    }

    /// The non-negotiable for DMs: two bots DMing each other must hit the same
    /// hard cap as in a channel, instead of spiraling unboundedly. Drive a
    /// realistic bot-to-bot DM exchange through classify() -> sender_is_bot() ->
    /// should_respond() keyed by the DM guard key and assert the cap fires.
    #[test]
    fn bot_to_bot_dm_cap_fires_end_to_end() {
        let cfg = IrcConfig::default(); // empty humans => unknown sender is a bot
        let bot_nick = "darbot";
        let mut guard = LoopGuard::new(cfg.effective_max_bot_turns());

        let mut allowed_replies = 0u32;
        for _ in 0..(cfg.effective_max_bot_turns() + 10) {
            // Another agent DMs our bot (target is our own nick => DM).
            let pm = PrivMsg {
                sender: "otheragent".into(),
                target: bot_nick.into(),
                text: "keep going".into(),
            };
            let (verdict, conv) = classify(&pm, bot_nick, true);
            assert_eq!(verdict, Verdict::Reply);
            let Conversation::Dm(_) = &conv else {
                panic!("expected DM conversation");
            };
            // DM allowlist is empty => authorized, so the guard is the only stop.
            assert!(dm_authorized(&pm.sender, &cfg.allowed_users));
            let is_bot = sender_is_bot(&pm.sender, &cfg.humans);
            assert!(is_bot, "unknown DM sender must count as a bot");
            if guard.should_respond(is_bot, &conv.guard_key()) {
                allowed_replies += 1;
            }
        }
        // The DM path must be bounded, never unbounded.
        assert_eq!(allowed_replies, cfg.effective_max_bot_turns());
    }

    /// A DM guard key and a same-named channel must not collide: capping a DM
    /// from nick `room` must not mute channel `#room` or vice versa.
    #[test]
    fn dm_and_channel_guard_keys_are_isolated() {
        let chan = Conversation::Channel("#room".into());
        let dm = Conversation::Dm("room".into());
        assert_ne!(chan.guard_key(), dm.guard_key());
    }

    /// A known human keeps the exchange uncapped and resets the counter, while a
    /// populated DM allowlist does NOT affect channel bot classification.
    #[test]
    fn known_human_resets_cap_independent_of_dm_allowlist() {
        let cfg = IrcConfig {
            allowed_users: vec!["carol".to_string()], // DM gate only
            humans: vec!["alice".to_string()],
            ..IrcConfig::default()
        };
        let mut guard = LoopGuard::new(2);

        let bot_msg = sender_is_bot("otheragent", &cfg.humans);
        let human_msg = sender_is_bot("alice", &cfg.humans);
        assert!(bot_msg);
        assert!(!human_msg);
        // A human NOT on the DM allowlist is still correctly seen as a human.
        assert!(!sender_is_bot("alice", &cfg.humans));

        assert!(guard.should_respond(bot_msg, "#room"));
        assert!(guard.should_respond(bot_msg, "#room"));
        assert!(!guard.should_respond(bot_msg, "#room")); // capped
        assert!(guard.should_respond(human_msg, "#room")); // human resets
        assert_eq!(guard.count("#room"), 0);
        assert!(guard.should_respond(bot_msg, "#room")); // allowed again
    }
}
