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
use dar_extension_sdk::chat::{ChatBackend, ChatEvent, ChatRole, ChatSession};
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

/// Reconnect backoff bounds.
const BACKOFF_MIN: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Brief window for a one-shot outbound tool call to observe immediate IRC
/// rejection numerics after writing PRIVMSG.
const OUTBOUND_ERROR_WAIT: Duration = Duration::from_millis(750);
/// The pickup acknowledgement sent once a message burst has been coalesced,
/// right before the turn runs.
const ACK_TEXT: &str = "👀";

pub struct IrcExtension;

pub fn extension() -> Box<dyn Extension> {
    Box::new(IrcExtension)
}

impl Extension for IrcExtension {
    fn id(&self) -> &'static str {
        "irc"
    }

    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
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
                registry
                    .register_tool(irc_send_spec(), Arc::new(IrcSendTool { cfg: cfg.clone() }))?;
            }
            Ok(())
        })
    }

    fn agent_singleton(&self) -> bool {
        true
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

/// A classified inbound message forwarded from the socket read loop to the turn
/// worker. Keeping classification light (pure, no I/O) on the read side and all
/// session/turn work on the worker side is what keeps the socket serviced (PINGs
/// answered) while a long agent turn runs — the fix for the `Broken pipe` drops.
struct Incoming {
    pm: PrivMsg,
    conv: Conversation,
    verdict: Verdict,
    bot_nick: String,
}

/// Control messages the worker consumes. `Msg` is a classified inbound line;
/// `Sender` installs the writer half of a freshly (re)connected link so replies
/// produced by in-flight or subsequently completed turns are delivered over the
/// live socket rather than a dead one.
enum WorkerMsg {
    Msg(Incoming),
    Sender(conn::Sender),
}

/// A reply ready for delivery, addressed to an exact IRC target. Queued when no
/// healthy sender is available (e.g. the link died mid-turn) and flushed the
/// moment a reconnected sender arrives, so a completed turn is retried instead of
/// being silently lost.
struct PendingReply {
    target: String,
    text: String,
}

async fn run(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    cfg: &IrcConfig,
    session_dir: &Path,
) -> Result<()> {
    let mut backoff = BACKOFF_MIN;

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

    // The turn worker runs for the whole extension lifetime, independent of any
    // single connection. It owns all sessions and the loop-guard, runs agent
    // turns off the socket read path, and delivers replies through whichever
    // sender is currently live. Persisting it across reconnects is what lets a
    // reply produced during a dropped link be retried on the next one.
    //
    // The queue is generously bounded: while the worker is inside a long turn it
    // isn't draining, so inbound lines buffer here. 256 comfortably absorbs any
    // realistic burst (IRC servers flood-kick long before that), so in practice
    // the read loop never blocks on `send` and the PING path is never re-starved.
    let (worker_tx, worker_rx) = mpsc::channel::<WorkerMsg>(256);
    let worker = {
        let ctx = ctx.clone();
        let cfg = cfg.clone();
        let session_dir = session_dir.to_path_buf();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            worker_loop(&ctx, &mut shutdown, &cfg, &session_dir, worker_rx).await;
        })
    };

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
        // Hand the worker the fresh sender so it (and any queued reply) targets
        // the live link. If the worker is gone the extension is shutting down.
        if worker_tx
            .send(WorkerMsg::Sender(conn.sender()))
            .await
            .is_err()
        {
            break;
        }
        match serve(cfg, shutdown, &worker_tx, conn).await {
            Ok(true) => break, // graceful shutdown
            Ok(false) => {
                tracing::warn!("irc connection closed; reconnecting");
            }
            Err(err) => {
                tracing::warn!(error = %err, "irc connection error; reconnecting");
            }
        }
    }

    // Drop the worker channel so the worker loop exits, then await it.
    drop(worker_tx);
    let _ = worker.await;
    Ok(())
}

/// Serve one live connection: a pure socket read loop. It parses and classifies
/// each line (cheap, no I/O) and forwards it to the turn worker, then immediately
/// loops back to read again — so server PINGs are answered inside
/// [`Connection::next_message`] even while a long agent turn is in flight. This
/// is the core of the fix: the read loop never blocks on a turn, so the link
/// stays healthy and outbound replies no longer hit a `Broken pipe`.
///
/// Returns `Ok(true)` on graceful shutdown, `Ok(false)` on a clean disconnect
/// (reconnect), or `Err` on a read error (reconnect).
async fn serve(
    cfg: &IrcConfig,
    shutdown: &mut ShutdownToken,
    worker_tx: &mpsc::Sender<WorkerMsg>,
    mut conn: Connection,
) -> Result<bool> {
    loop {
        let msg = tokio::select! {
            _ = shutdown.cancelled() => return Ok(true),
            next = conn.next_message() => match next? {
                Some(m) => m,
                None => return Ok(false),
            }
        };

        // Read the live nick fresh each line: a server-initiated NICK change
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
        if verdict == Verdict::Ignore {
            continue;
        }

        let incoming = Incoming {
            pm,
            conv,
            verdict,
            bot_nick,
        };
        if worker_tx.send(WorkerMsg::Msg(incoming)).await.is_err() {
            // Worker gone => extension shutting down.
            return Ok(true);
        }
    }
}

/// The turn worker: owns every session and the loop-guard, and drives agent turns
/// entirely off the socket read path. It coalesces rapid successive lines from the
/// same conversation (debounce) into one prompt so a pasted multi-line DM becomes
/// a single turn instead of several serial ones, then delivers the reply through
/// the current live sender — queuing and retrying it across a reconnect if the
/// link died mid-turn.
async fn worker_loop(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    cfg: &IrcConfig,
    session_dir: &Path,
    mut rx: mpsc::Receiver<WorkerMsg>,
) {
    let mut state = ChannelState {
        sessions: HashMap::new(),
        guard: LoopGuard::new(cfg.effective_max_bot_turns()),
    };
    let mut sender: Option<conn::Sender> = None;
    let mut pending: VecDeque<PendingReply> = VecDeque::new();
    let debounce = cfg.effective_debounce();
    // A message pulled off the queue by the coalescer that turned out to belong
    // to a different conversation is carried here and processed first next loop,
    // so nothing read from the socket is ever dropped.
    let mut carry: Option<Incoming> = None;

    loop {
        let incoming = if let Some(carried) = carry.take() {
            carried
        } else {
            let msg = tokio::select! {
                _ = shutdown.cancelled() => return,
                next = rx.recv() => match next {
                    Some(m) => m,
                    None => return,
                }
            };
            match msg {
                WorkerMsg::Sender(new_sender) => {
                    sender = Some(new_sender);
                    // A healthy link just arrived: flush anything that failed to
                    // send (or was produced) while we had no live sender.
                    flush_pending(&mut pending, &sender).await;
                    continue;
                }
                WorkerMsg::Msg(incoming) => incoming,
            }
        };

        match incoming.verdict {
            Verdict::Ignore => continue,
            Verdict::ContextOnly => {
                ingest_context(ctx, &mut state, cfg, session_dir, &incoming).await;
            }
            Verdict::Reply => {
                carry = handle_reply(
                    ctx,
                    shutdown,
                    &mut state,
                    cfg,
                    session_dir,
                    &mut rx,
                    &mut sender,
                    &mut pending,
                    debounce,
                    incoming,
                )
                .await;
            }
        }
    }
}

/// Ingest an ambient (context-only) channel line: a human resets the loop-guard,
/// and the line is buffered as bounded context for the next turn.
async fn ingest_context(
    ctx: &StartCtx,
    state: &mut ChannelState,
    cfg: &IrcConfig,
    session_dir: &Path,
    incoming: &Incoming,
) {
    if let Conversation::Channel(ch) = &incoming.conv {
        if !sender_is_bot(&incoming.pm.sender, &cfg.humans) {
            state.guard.note_human(ch);
        }
    }
    buffer_ambient(
        state,
        &incoming.conv,
        &incoming.pm,
        cfg.effective_context_window(),
        ctx,
        session_dir,
        cfg,
    )
    .await;
}

/// Handle a message that addresses the bot: authorize, apply the loop-guard,
/// coalesce any rapid follow-up lines from the same conversation, run the turn
/// off the read path, and deliver (or queue) the reply.
/// Returns any message the coalescer pulled off the queue that did not belong to
/// this burst, so the worker loop can process it next instead of dropping it.
#[allow(clippy::too_many_arguments)]
async fn handle_reply(
    ctx: &StartCtx,
    shutdown: &mut ShutdownToken,
    state: &mut ChannelState,
    cfg: &IrcConfig,
    session_dir: &Path,
    rx: &mut mpsc::Receiver<WorkerMsg>,
    sender: &mut Option<conn::Sender>,
    pending: &mut VecDeque<PendingReply>,
    debounce: Duration,
    incoming: Incoming,
) -> Option<Incoming> {
    let Incoming {
        pm, conv, bot_nick, ..
    } = incoming;

    if let Conversation::Dm(nick) = &conv {
        if !dm_authorized(nick, &cfg.allowed_users) {
            tracing::info!(nick, "irc DM from non-allowlisted nick; ignoring");
            return None;
        }
    }

    // Loop-guard: cap consecutive bot-to-bot turns. Applies to BOTH channels AND
    // DMs — a DM is an unattended channel, so two agents DMing each other must hit
    // the same hard cap (the non-negotiable: no runaway bot-to-bot cost with no
    // human present).
    let is_bot = sender_is_bot(&pm.sender, &cfg.humans);
    let guard_key = conv.guard_key();
    if !state.guard.should_respond(is_bot, &guard_key) {
        tracing::info!(
            conversation = %guard_key,
            consecutive_bot_turns = state.guard.count(&guard_key),
            "irc bot-to-bot cap reached; staying silent"
        );
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
        return None;
    }

    dar_extension_sdk::log::event(
        "-",
        "irc",
        &format!("message from {} in {}", pm.sender, guard_key),
    );
    let target = conv.reply_target(&pm.sender);

    // Coalesce rapid successive lines from the SAME conversation AND sender into
    // one prompt. A pasted multi-line DM (or channel message) arrives as several
    // PRIVMSGs back-to-back; without this each line would spawn its own serial
    // turn (the accidental serial-turn bug). We buffer the addressed line, then
    // briefly wait for more lines from the same conversation and sender, folding
    // each into the prompt and resetting the timer.
    //
    // Keyed by (conversation, sender) rather than conversation alone: a DM
    // conversation is already exactly one sender, but a channel conversation is
    // keyed by channel name and can hold many humans, so the sender check keeps
    // two different humans' channel messages from ever being merged into one
    // turn. Only the first line of a channel paste carries the `nick:` mention
    // prefix — the rest classify as ContextOnly — so same-sender ContextOnly
    // lines within the window fold in too (see `coalesce_followups`).
    let mut request_lines = vec![strip_mention(&pm.text, &bot_nick).to_string()];
    let mut carry = None;
    if !debounce.is_zero() {
        carry = coalesce_followups(
            rx,
            &conv,
            &pm.sender,
            &bot_nick,
            debounce,
            &mut request_lines,
            sender,
        )
        .await;
    }

    // Pickup ack: send a `👀` once the full burst has been coalesced, right
    // before the turn runs — this lands after a multi-line paste instead of in
    // the middle of it. Best-effort: a failed send is logged and swallowed so it
    // can never block or delay the turn.
    if should_ack(cfg.effective_ack()) {
        if let Some(s) = sender.as_ref() {
            if let Err(err) = send_reply(s, &target, ACK_TEXT).await {
                tracing::warn!(error = %err, target, "irc pickup ack failed");
            }
        }
    }

    let prompt = build_prompt(state, &conv, &pm, &request_lines);
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

    deliver(sender, pending, &target, reply).await;
    carry
}

/// During the debounce window, drain further lines addressed to the SAME
/// conversation AND SAME sender (case-insensitive nick match) and fold them into
/// `request_lines`. A matching `Reply` line folds in as-is; a matching
/// `ContextOnly` line also folds in, since a channel paste's second and later
/// lines lack the `nick:` mention prefix and would otherwise classify as
/// ContextOnly and be misread as ambient chatter. Anything else — a different
/// conversation, a different sender, or an unrelated verdict — is not consumed
/// here: it is handed back as carry so the worker processes it on the next loop
/// instead of dropping it. Each matching line resets the window.
async fn coalesce_followups(
    rx: &mut mpsc::Receiver<WorkerMsg>,
    conv: &Conversation,
    burst_sender: &str,
    bot_nick: &str,
    debounce: Duration,
    request_lines: &mut Vec<String>,
    sender: &mut Option<conn::Sender>,
) -> Option<Incoming> {
    loop {
        match tokio::time::timeout(debounce, rx.recv()).await {
            Ok(Some(WorkerMsg::Msg(next))) => {
                let same_burst = &next.conv == conv
                    && next.pm.sender.eq_ignore_ascii_case(burst_sender)
                    && matches!(next.verdict, Verdict::Reply | Verdict::ContextOnly);
                if same_burst {
                    request_lines.push(strip_mention(&next.pm.text, bot_nick).to_string());
                    // Keep waiting: this resets the window by looping.
                } else {
                    // Not part of this burst: hand it back so the worker
                    // processes it next instead of dropping it.
                    return Some(next);
                }
            }
            // A (re)connect landed mid-burst: adopt the fresh sender so the reply
            // targets the live link, and keep coalescing.
            Ok(Some(WorkerMsg::Sender(new_sender))) => {
                *sender = Some(new_sender);
            }
            // Window elapsed or channel closed: burst complete.
            Ok(None) | Err(_) => return None,
        }
    }
}

/// Deliver a completed reply over the live sender, or queue it for retry if no
/// healthy sender is available or the send fails. This is what turns a mid-turn
/// link drop from "silently lost response" into "delivered after reconnect".
async fn deliver(
    sender: &Option<conn::Sender>,
    pending: &mut VecDeque<PendingReply>,
    target: &str,
    reply: String,
) {
    match sender.as_ref() {
        Some(s) => {
            if let Err(err) = send_reply(s, target, &reply).await {
                tracing::warn!(error = %err, target, "irc PRIVMSG failed; queuing for retry");
                pending.push_back(PendingReply {
                    target: target.to_string(),
                    text: reply,
                });
            }
        }
        None => {
            tracing::warn!(target, "irc has no live link; queuing reply for retry");
            pending.push_back(PendingReply {
                target: target.to_string(),
                text: reply,
            });
        }
    }
}

/// Flush queued replies over a freshly installed sender. Any that still fail are
/// re-queued (preserving order) so the next reconnect retries them.
async fn flush_pending(pending: &mut VecDeque<PendingReply>, sender: &Option<conn::Sender>) {
    let Some(s) = sender.as_ref() else {
        return;
    };
    let mut retry = VecDeque::new();
    while let Some(reply) = pending.pop_front() {
        if let Err(err) = send_reply(s, &reply.target, &reply.text).await {
            tracing::warn!(error = %err, target = %reply.target, "irc retry send failed; will retry");
            retry.push_back(reply);
        } else {
            tracing::info!(target = %reply.target, "irc delivered queued reply after reconnect");
        }
    }
    *pending = retry;
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

/// Decide whether a picked-up message should get a `👀` pickup ack once its
/// burst has been coalesced. The caller invokes this post-gate (after the loop
/// guard and after coalescing) so a `true` here implies a reply will follow.
fn should_ack(ack_enabled: bool) -> bool {
    ack_enabled
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
/// the addressed request. `request_lines` are the already-mention-stripped lines
/// of the (possibly coalesced) burst; multiple lines are joined with newlines so
/// a pasted multi-line DM reads as one coherent request.
fn build_prompt(
    state: &mut ChannelState,
    conv: &Conversation,
    pm: &PrivMsg,
    request_lines: &[String],
) -> String {
    let request = request_lines
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
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
        "401"
            | "403"
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
    params
        .iter()
        .any(|param| param.eq_ignore_ascii_case(target))
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

async fn open_session(
    ctx: &StartCtx,
    session_dir: &Path,
    configured: Option<&str>,
) -> Result<ChatConn> {
    // Resolve the backend and build params through the shared SDK helpers so
    // IRC talks to the same agent identity as TUI chat: model/provider from the
    // retained RunSnapshot, the retained `system.context` as the system prompt,
    // and the host tool bridge. Keeping this on the SDK path means a new chat
    // surface inherits the agent's system prompt by default instead of having
    // to remember to copy it (the bug this fixes: IRC opened sessions without
    // a system prompt and the provider rejected them).
    let backend_id = dar_extension_sdk::chat::resolve_agent_backend(ctx, configured);
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;

    let params = dar_extension_sdk::chat::agent_session_params(ctx, session_dir).build();

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
    use tokio::io::AsyncReadExt;

    fn pm(sender: &str, target: &str, text: &str) -> PrivMsg {
        PrivMsg {
            sender: sender.into(),
            target: target.into(),
            text: text.into(),
        }
    }

    fn reply_incoming(sender: &str, bot_nick: &str, text: &str) -> Incoming {
        // A DM addressed to the bot: verdict Reply, keyed by sender.
        let p = pm(sender, bot_nick, text);
        let (verdict, conv) = classify(&p, bot_nick, true);
        Incoming {
            pm: p,
            conv,
            verdict,
            bot_nick: bot_nick.to_string(),
        }
    }

    /// A `#room` channel message classified against `bot_nick`: `Reply` when
    /// `text` leads with the mention, `ContextOnly` otherwise (the shape of the
    /// second and later lines of a channel paste).
    fn channel_incoming(sender: &str, bot_nick: &str, text: &str) -> Incoming {
        let p = pm(sender, "#room", text);
        let (verdict, conv) = classify(&p, bot_nick, true);
        Incoming {
            pm: p,
            conv,
            verdict,
            bot_nick: bot_nick.to_string(),
        }
    }

    /// A fresh ChannelState with no known humans (default config) for prompt tests.
    fn empty_state() -> ChannelState {
        ChannelState {
            sessions: HashMap::new(),
            guard: LoopGuard::new(4),
        }
    }

    /// Rapid successive lines from the SAME conversation are coalesced within the
    /// debounce window into one multi-line prompt — the multi-line-DM fix. The
    /// three lines arrive back-to-back and must fold into a single request.
    #[tokio::test(start_paused = true)]
    async fn coalesce_folds_rapid_same_conversation_lines() {
        let (tx, mut rx) = mpsc::channel::<WorkerMsg>(16);
        // Enqueue two rapid follow-ups from the same DM sender.
        tx.send(WorkerMsg::Msg(reply_incoming(
            "thinh",
            "darbot",
            "Deepseek is...",
        )))
        .await
        .unwrap();
        tx.send(WorkerMsg::Msg(reply_incoming(
            "thinh",
            "darbot",
            "Test with 40 tweets",
        )))
        .await
        .unwrap();
        drop(tx); // close so the window ends after draining

        let conv = Conversation::Dm("thinh".into());
        let mut lines = vec!["First, verify".to_string()];
        let mut sender: Option<conn::Sender> = None;
        let carry = coalesce_followups(
            &mut rx,
            &conv,
            "thinh",
            "darbot",
            Duration::from_millis(1500),
            &mut lines,
            &mut sender,
        )
        .await;
        assert!(carry.is_none(), "no cross-conversation leftover expected");
        assert_eq!(
            lines,
            vec!["First, verify", "Deepseek is...", "Test with 40 tweets"]
        );

        let mut state = empty_state();
        let prompt = build_prompt(&mut state, &conv, &pm("thinh", "darbot", "x"), &lines);
        assert_eq!(
            prompt,
            "<thinh> First, verify\nDeepseek is...\nTest with 40 tweets"
        );
    }

    /// A line for a DIFFERENT conversation encountered during the window is not
    /// dropped: it is handed back as carry-over so the worker processes it next.
    #[tokio::test(start_paused = true)]
    async fn coalesce_returns_cross_conversation_line_as_carry() {
        let (tx, mut rx) = mpsc::channel::<WorkerMsg>(16);
        tx.send(WorkerMsg::Msg(reply_incoming(
            "alice", "darbot", "other DM",
        )))
        .await
        .unwrap();
        drop(tx);

        let conv = Conversation::Dm("thinh".into());
        let mut lines = vec!["hello".to_string()];
        let mut sender: Option<conn::Sender> = None;
        let carry = coalesce_followups(
            &mut rx,
            &conv,
            "thinh",
            "darbot",
            Duration::from_millis(1500),
            &mut lines,
            &mut sender,
        )
        .await;
        let carried = carry.expect("cross-conversation line must be carried, not dropped");
        assert_eq!(carried.pm.sender, "alice");
        assert_eq!(lines, vec!["hello"]); // unchanged: not folded in
    }

    /// A channel paste from the SAME sender folds in even though only its first
    /// line carries the mention: the second line classifies as ContextOnly, not
    /// Reply, and must still be coalesced (otherwise it would leak into the
    /// context ring instead of the prompt).
    #[tokio::test(start_paused = true)]
    async fn coalesce_folds_channel_paste_from_same_sender() {
        let (tx, mut rx) = mpsc::channel::<WorkerMsg>(16);
        // A same-sender follow-up with no mention: ContextOnly.
        tx.send(WorkerMsg::Msg(channel_incoming(
            "thinh",
            "darbot",
            "plain follow-up line",
        )))
        .await
        .unwrap();
        // A same-sender follow-up that re-addresses the bot: Reply.
        tx.send(WorkerMsg::Msg(channel_incoming(
            "thinh",
            "darbot",
            "darbot: more",
        )))
        .await
        .unwrap();
        drop(tx);

        let conv = Conversation::Channel("#room".into());
        let mut lines = vec!["darbot: first line".to_string()];
        let mut sender: Option<conn::Sender> = None;
        let carry = coalesce_followups(
            &mut rx,
            &conv,
            "thinh",
            "darbot",
            Duration::from_millis(1500),
            &mut lines,
            &mut sender,
        )
        .await;
        assert!(carry.is_none(), "same-sender channel burst must fully fold");
        assert_eq!(
            lines,
            vec!["darbot: first line", "plain follow-up line", "more"]
        );
    }

    /// A channel line from a DIFFERENT sender must never be folded into another
    /// human's burst, even mid-window — the fix that lets channel coalescing
    /// stay safe for multi-human rooms.
    #[tokio::test(start_paused = true)]
    async fn coalesce_carries_different_sender_channel_line() {
        let (tx, mut rx) = mpsc::channel::<WorkerMsg>(16);
        tx.send(WorkerMsg::Msg(channel_incoming(
            "alice",
            "darbot",
            "darbot: butting in",
        )))
        .await
        .unwrap();
        drop(tx);

        let conv = Conversation::Channel("#room".into());
        let mut lines = vec!["darbot: first line".to_string()];
        let mut sender: Option<conn::Sender> = None;
        let carry = coalesce_followups(
            &mut rx,
            &conv,
            "thinh",
            "darbot",
            Duration::from_millis(1500),
            &mut lines,
            &mut sender,
        )
        .await;
        let carried = carry.expect("different-sender channel line must be carried, not dropped");
        assert_eq!(carried.pm.sender, "alice");
        assert_eq!(lines, vec!["darbot: first line"]); // unchanged: not folded in
    }

    /// IRC nicks are case-insensitive: a follow-up from "THINH" still matches a
    /// burst opened by "thinh".
    #[tokio::test(start_paused = true)]
    async fn coalesce_sender_match_is_case_insensitive() {
        let (tx, mut rx) = mpsc::channel::<WorkerMsg>(16);
        tx.send(WorkerMsg::Msg(channel_incoming(
            "THINH",
            "darbot",
            "darbot: more",
        )))
        .await
        .unwrap();
        drop(tx);

        let conv = Conversation::Channel("#room".into());
        let mut lines = vec!["darbot: first line".to_string()];
        let mut sender: Option<conn::Sender> = None;
        let carry = coalesce_followups(
            &mut rx,
            &conv,
            "thinh",
            "darbot",
            Duration::from_millis(1500),
            &mut lines,
            &mut sender,
        )
        .await;
        assert!(
            carry.is_none(),
            "case-insensitive same-sender line must fold in"
        );
        assert_eq!(lines, vec!["darbot: first line", "more"]);
    }

    /// A completed reply produced with no live link is queued, then delivered on
    /// the next healthy sender — the "retry after reconnect instead of silently
    /// losing the completed response" acceptance criterion.
    #[tokio::test]
    async fn reply_is_queued_when_no_link_then_flushed_on_reconnect() {
        let mut pending: VecDeque<PendingReply> = VecDeque::new();
        // No sender: the completed reply must be queued, not lost.
        deliver(&None, &mut pending, "thinh", "the final answer".to_string()).await;
        assert_eq!(pending.len(), 1);

        // A reconnect brings a fresh sender: the queued reply flushes to the wire.
        let (sender, mut server) = conn::duplex_sender();
        flush_pending(&mut pending, &Some(sender)).await;
        assert!(pending.is_empty(), "queued reply must be delivered");

        let mut buf = vec![0u8; 256];
        let n = server.read(&mut buf).await.unwrap();
        let sent = String::from_utf8_lossy(&buf[..n]);
        assert!(
            sent.contains("PRIVMSG thinh :the final answer"),
            "unexpected wire output: {sent:?}"
        );
    }

    /// The debounce window is configurable and disable-able via `0`.
    #[test]
    fn debounce_config_resolves_and_disables_at_zero() {
        let default = IrcConfig::default();
        assert_eq!(
            default.effective_debounce(),
            Duration::from_millis(config::DEFAULT_DEBOUNCE_MS)
        );
        let off = IrcConfig {
            debounce_ms: Some(0),
            ..IrcConfig::default()
        };
        assert!(off.effective_debounce().is_zero());
    }

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
    fn should_ack_all_messages_when_enabled() {
        let humans = vec!["alice".to_string()];
        let cfg = IrcConfig {
            humans: humans.clone(),
            ..IrcConfig::default()
        };
        assert!(cfg.effective_ack());
        assert!(!sender_is_bot("alice", &humans));
        assert!(should_ack(cfg.effective_ack()));
        // A2A classification still feeds loop guard, but no longer excludes ack.
        assert!(sender_is_bot("otheragent", &humans));
        assert!(should_ack(cfg.effective_ack()));
    }

    #[test]
    fn should_ack_off_suppresses_all_messages() {
        let cfg = IrcConfig {
            ack: Some(false),
            ..IrcConfig::default()
        };
        assert!(!cfg.effective_ack());
        assert!(!should_ack(cfg.effective_ack()));
    }

    #[test]
    fn should_ack_empty_humans() {
        // Fail-closed classification still makes every sender a bot for the loop
        // guard, but does not affect acknowledgements.
        let cfg = IrcConfig::default();
        assert!(cfg.effective_ack());
        assert!(sender_is_bot("anyone", &cfg.humans));
        assert!(should_ack(cfg.effective_ack()));
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
