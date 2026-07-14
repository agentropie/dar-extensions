use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use dar_extension_sdk::{
    artifacts::{ArtifactDeliveryTarget, DeliveryClaimResult},
    chat::{ArtifactReady, ChatBackend, ChatEvent, ChatRole, ChatSession},
    StartCtx,
};

use crate::{
    addressing::{route, ConversationKind, InboundMessage, RouteDecision},
    api::{SlackClient, SlackIdentity},
    attachments::download_private_file,
    commands::{self, Command},
    config::SlackConfig,
    history::History,
    live_answer::LiveAnswer,
    mrkdwn,
    session::ConversationKey,
    thinking::Thinking,
};

const THINKING_REACTION: &str = "eyes";
const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;
const KEY_WORK_QUEUE_CAPACITY: usize = 64;
const MAX_KEYED_WORKERS: usize = 8;
const DEDUPE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_DEDUPE_ENTRIES: usize = 100;

struct ChatConn {
    session: Box<dyn ChatSession>,
    rx: mpsc::Receiver<ChatEvent>,
    artifacts: mpsc::Receiver<ArtifactReady>,
}

struct TurnDisplay {
    client: SlackClient,
    channel: String,
    thread_ts: Option<String>,
    show_thinking: bool,
    delete_thinking_on_complete: bool,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    #[serde(default)]
    envelope_id: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct EventPayload {
    #[serde(default)]
    event: serde_json::Value,
    #[serde(default)]
    event_id: String,
    #[serde(default)]
    team_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct File {
    #[serde(default)]
    name: String,
    #[serde(default)]
    mimetype: String,
    #[serde(default)]
    url_private_download: String,
}

#[derive(Debug, Clone)]
struct Incoming {
    team_id: String,
    channel_id: String,
    sender_id: String,
    text: String,
    ts: String,
    thread_ts: Option<String>,
    kind: ConversationKind,
    files: Vec<File>,
    is_reaction: bool,
}

enum Work {
    Message(Incoming),
    Command {
        command: Command,
        incoming: Incoming,
    },
}

enum Control {
    Stop(oneshot::Sender<bool>),
}

struct Worker {
    work: mpsc::Sender<Work>,
    control: mpsc::Sender<Control>,
}

struct WorkerEnv<'a> {
    ctx: &'a StartCtx,
    client: &'a SlackClient,
    cfg: &'a SlackConfig,
    patterns: &'a [regex::Regex],
    identity: &'a SlackIdentity,
    data_dir: &'a Path,
    history: &'a History,
}

#[derive(Clone)]
struct DispatcherEnv {
    ctx: StartCtx,
    client: SlackClient,
    cfg: SlackConfig,
    patterns: Vec<regex::Regex>,
    identity: SlackIdentity,
    data_dir: PathBuf,
    history: Arc<History>,
}

/// Socket Mode receive loop. Envelope acknowledgement is delegated to an
/// independent writer task; model and download work run through keyed workers.
pub async fn run(ctx: StartCtx, cfg: SlackConfig) -> Result<()> {
    let tokens = cfg.tokens()?;
    let data_dir = ctx.paths.data_dir("slack")?;
    std::fs::create_dir_all(data_dir.join("sessions"))?;
    std::fs::create_dir_all(ctx.paths.root().join("data/uploads"))?;

    let client = SlackClient::new(tokens.bot)?;
    let identity = client.auth_test().await.context("Slack auth.test failed")?;
    let patterns = cfg.compiled_mention_patterns()?;
    let dispatcher_env = DispatcherEnv {
        ctx: ctx.clone(),
        client: client.clone(),
        cfg: cfg.clone(),
        patterns: patterns.clone(),
        identity: identity.clone(),
        data_dir: data_dir.clone(),
        history: Arc::new(History::default()),
    };
    let mut workers = HashMap::<String, Worker>::new();
    let mut worker_tasks = Vec::new();
    let (idle_worker_tx, mut idle_worker_rx) = mpsc::unbounded_channel();

    let mut shutdown = ctx.shutdown.clone();
    let mut delay = Duration::from_secs(1);
    let mut dedupe = HashMap::<String, Instant>::new();
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let url = match client.socket_mode_url(&tokens.app).await {
            Ok(url) => url,
            Err(error) => {
                dar_extension_sdk::log::event(
                    "-",
                    "slack",
                    &format!("Socket Mode unavailable: {error}"),
                );
                wait_or_shutdown(&mut shutdown, delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let (socket, _) = match tokio_tungstenite::connect_async(url).await {
            Ok(socket) => socket,
            Err(error) => {
                dar_extension_sdk::log::event(
                    "-",
                    "slack",
                    &format!("Socket Mode connect failed: {error}"),
                );
                wait_or_shutdown(&mut shutdown, delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        delay = Duration::from_secs(1);
        let (mut write, mut read) = socket.split();
        let mut hello_logged = false;
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<String>();
        let mut writer_shutdown = shutdown.clone();
        let writer = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = writer_shutdown.cancelled() => return,
                    message = ack_rx.recv() => match message {
                        Some(message) => { if write.send(Message::Text(message)).await.is_err() { return; } }
                        None => return,
                    }
                }
            }
        });

        loop {
            let frame = tokio::select! {
                _ = shutdown.cancelled() => None,
                frame = read.next() => frame,
            };
            let Some(Ok(frame)) = frame else { break };
            let Ok(text) = frame.into_text() else {
                continue;
            };
            let Ok(envelope) = serde_json::from_str::<Envelope>(&text) else {
                continue;
            };
            if should_log_socket_mode_hello(&envelope, &mut hello_logged) {
                dar_extension_sdk::log::event("-", "slack", "Socket Mode connected.");
                continue;
            }
            let Some(key) = dedupe_key(&envelope) else {
                continue;
            };
            prune_dedupe(&mut dedupe);
            if dedupe.insert(key, Instant::now()).is_some() {
                if !envelope.envelope_id.is_empty() {
                    let _ = ack_tx.send(ack(&envelope.envelope_id, None));
                }
                continue;
            }
            trim_dedupe(&mut dedupe);
            if let Some((command, incoming)) = slash_command(&envelope) {
                let response: String = if !command_allowed(&cfg, &incoming) {
                    "Access denied.".into()
                } else if command == Command::Stop {
                    if stop_active_session(&mut workers, &incoming).await {
                        commands::reply(command).into()
                    } else {
                        "No active response to stop.".into()
                    }
                } else if command == Command::Ping {
                    match client.auth_test().await {
                        Ok(current) if current.team_id == identity.team_id => "Bot healthy.".into(),
                        Ok(_) => "Bot health check failed: workspace mismatch.".into(),
                        Err(_) => "Bot health check failed.".into(),
                    }
                } else if !matches!(command, Command::Help)
                    && !enqueue_work(
                        &mut workers,
                        &mut worker_tasks,
                        &mut idle_worker_rx,
                        &idle_worker_tx,
                        Work::Command { command, incoming },
                        &dispatcher_env,
                    )
                {
                    "Agent is busy; retry shortly.".into()
                } else {
                    commands::reply(command).into()
                };
                if !envelope.envelope_id.is_empty() {
                    let _ = ack_tx.send(ack(&envelope.envelope_id, Some(&response)));
                }
                continue;
            }
            if !envelope.envelope_id.is_empty() {
                let _ = ack_tx.send(ack(&envelope.envelope_id, None));
            }
            let Some(mut incoming) = incoming(&envelope) else {
                continue;
            };
            resolve_reaction_thread(&client, &mut incoming).await;
            if !accepted_message(&cfg, &identity, &patterns, &incoming) {
                continue;
            }
            dar_extension_sdk::log::event("-", "slack", &accepted_message_log(&incoming));
            if message_command(&incoming, &identity, &patterns) == Some(Command::Stop) {
                let response = if stop_active_session(&mut workers, &incoming).await {
                    commands::reply(Command::Stop)
                } else {
                    "No active response to stop."
                };
                let _ = client
                    .post_message(
                        &incoming.channel_id,
                        response,
                        incoming.thread_ts.as_deref(),
                    )
                    .await;
                continue;
            }
            let thinking =
                cfg.show_thinking && message_command(&incoming, &identity, &patterns).is_none();
            if thinking {
                let _ = client
                    .add_reaction(&incoming.channel_id, &incoming.ts, THINKING_REACTION)
                    .await;
            }
            if !enqueue_work(
                &mut workers,
                &mut worker_tasks,
                &mut idle_worker_rx,
                &idle_worker_tx,
                Work::Message(incoming.clone()),
                &dispatcher_env,
            ) {
                if thinking {
                    clear_thinking_reaction(&client, &incoming).await;
                }
                let busy_client = client.clone();
                tokio::spawn(async move {
                    let _ = busy_client
                        .post_message(
                            &incoming.channel_id,
                            "Agent is busy; retry shortly.",
                            incoming.thread_ts.as_deref(),
                        )
                        .await;
                });
            }
        }
        writer.abort();
        let _ = writer.await;
    }
    drop(workers);
    for worker in worker_tasks {
        let _ = worker.await;
    }
    Ok(())
}

fn enqueue_work(
    workers: &mut HashMap<String, Worker>,
    worker_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    idle_worker_rx: &mut mpsc::UnboundedReceiver<String>,
    idle_worker_tx: &mpsc::UnboundedSender<String>,
    work: Work,
    dispatcher_env: &DispatcherEnv,
) -> bool {
    while let Ok(key) = idle_worker_rx.try_recv() {
        workers.remove(&key);
    }
    worker_tasks.retain(|task| !task.is_finished());
    let key = work_key(&work);
    if let Some(worker) = workers.get(&key) {
        return worker.work.try_send(work).is_ok();
    }
    if workers.len() >= MAX_KEYED_WORKERS {
        return false;
    }

    let (worker_tx, mut worker_rx) = mpsc::channel(KEY_WORK_QUEUE_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    if worker_tx.try_send(work).is_err() {
        return false;
    }
    let worker_key = key.clone();
    workers.insert(
        key,
        Worker {
            work: worker_tx,
            control: control_tx,
        },
    );
    let idle_worker_tx = idle_worker_tx.clone();
    let worker_ctx = dispatcher_env.ctx.clone();
    let worker_client = dispatcher_env.client.clone();
    let worker_cfg = dispatcher_env.cfg.clone();
    let worker_patterns = dispatcher_env.patterns.clone();
    let worker_identity = dispatcher_env.identity.clone();
    let worker_data_dir = dispatcher_env.data_dir.clone();
    let worker_history = dispatcher_env.history.clone();
    worker_tasks.push(tokio::spawn(async move {
        let mut worker_shutdown = worker_ctx.shutdown.clone();
        let env = WorkerEnv {
            ctx: &worker_ctx,
            client: &worker_client,
            cfg: &worker_cfg,
            patterns: &worker_patterns,
            identity: &worker_identity,
            data_dir: &worker_data_dir,
            history: &worker_history,
        };
        let mut sessions = HashMap::new();
        loop {
            tokio::select! {
                _ = worker_shutdown.cancelled() => break,
                work = worker_rx.recv() => match work {
                    Some(work) => run_work(&env, &mut sessions, &mut control_rx, work).await,
                    None => break,
                },
                control = control_rx.recv() => match control {
                    Some(Control::Stop(reply)) => { let _ = reply.send(false); }
                    None => break,
                },
                _ = tokio::time::sleep(Duration::from_secs(60)) => break,
            }
        }
        for connection in sessions.values_mut() {
            let _ = connection.session.abort().await;
        }
        let _ = idle_worker_tx.send(worker_key);
    }));
    true
}

async fn stop_active_session(workers: &mut HashMap<String, Worker>, incoming: &Incoming) -> bool {
    let key = ConversationKey::from_message(&inbound(incoming, ""))
        .as_str()
        .to_owned();
    let Some(worker) = workers.get_mut(&key) else {
        return false;
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    if worker.control.try_send(Control::Stop(reply_tx)).is_err() {
        return false;
    }
    reply_rx.await.unwrap_or(false)
}

fn message_command(
    incoming: &Incoming,
    identity: &SlackIdentity,
    patterns: &[regex::Regex],
) -> Option<Command> {
    let (_, text) =
        crate::addressing::strip_mention(&incoming.text, Some(&identity.user_id), patterns);
    text.trim_start()
        .strip_prefix('!')
        .and_then(|_| commands::parse(&text))
}

fn accepted_message(
    cfg: &SlackConfig,
    identity: &SlackIdentity,
    patterns: &[regex::Regex],
    incoming: &Incoming,
) -> bool {
    if incoming.team_id != identity.team_id {
        return false;
    }
    let reaction_text = incoming
        .is_reaction
        .then(|| format!("<@{}> {}", identity.user_id, incoming.text));
    let message = InboundMessage {
        text: reaction_text.as_deref().unwrap_or(&incoming.text),
        ..inbound(incoming, &identity.user_id)
    };
    matches!(
        route(cfg, patterns, &message),
        RouteDecision::Dispatch { .. }
    )
}

fn accepted_message_log(incoming: &Incoming) -> String {
    format!(
        "message from channel {} (user {})",
        safe_slack_identifier(&incoming.channel_id),
        safe_slack_identifier(&incoming.sender_id),
    )
}

fn safe_slack_identifier(identifier: &str) -> String {
    let safe: String = identifier
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(128)
        .collect();
    if safe.is_empty() {
        "?".into()
    } else {
        safe
    }
}

fn work_key(work: &Work) -> String {
    let incoming = match work {
        Work::Message(incoming) | Work::Command { incoming, .. } => incoming,
    };
    ConversationKey::from_message(&inbound(incoming, ""))
        .as_str()
        .to_owned()
}

async fn run_work(
    env: &WorkerEnv<'_>,
    sessions: &mut HashMap<String, ChatConn>,
    control_rx: &mut mpsc::Receiver<Control>,
    work: Work,
) {
    match work {
        Work::Message(incoming) => handle_message(env, sessions, control_rx, incoming).await,
        Work::Command { command, incoming } => {
            handle_command(env, sessions, command, incoming).await
        }
    }
}

async fn wait_or_shutdown(shutdown: &mut dar_extension_sdk::ShutdownToken, delay: Duration) {
    tokio::select! { _ = shutdown.cancelled() => {}, _ = tokio::time::sleep(delay) => {} }
}

fn ack(envelope_id: &str, text: Option<&str>) -> String {
    let mut value = serde_json::json!({"envelope_id": envelope_id});
    if let Some(text) = text {
        value["payload"] = serde_json::json!({"response_type":"ephemeral","text":text});
    }
    value.to_string()
}

fn should_log_socket_mode_hello(envelope: &Envelope, hello_logged: &mut bool) -> bool {
    if envelope.kind != "hello" || *hello_logged {
        return false;
    }
    *hello_logged = true;
    true
}

fn dedupe_key(envelope: &Envelope) -> Option<String> {
    // Slack retries may use a new Socket Mode envelope for same Events API
    // event; event_id is authoritative when available.
    if envelope.kind == "events_api" {
        if let Some(key) = serde_json::from_value::<EventPayload>(envelope.payload.clone())
            .ok()
            .filter(|payload| !payload.event_id.is_empty())
            .map(|payload| format!("event:{}", payload.event_id))
        {
            return Some(key);
        }
    }
    (!envelope.envelope_id.is_empty()).then(|| format!("envelope:{}", envelope.envelope_id))
}

fn prune_dedupe(dedupe: &mut HashMap<String, Instant>) {
    dedupe.retain(|_, seen| seen.elapsed() < DEDUPE_TTL);
}

fn trim_dedupe(dedupe: &mut HashMap<String, Instant>) {
    while dedupe.len() > MAX_DEDUPE_ENTRIES {
        let Some(key) = dedupe
            .iter()
            .min_by_key(|(_, seen)| **seen)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        dedupe.remove(&key);
    }
}

fn slash_command(envelope: &Envelope) -> Option<(Command, Incoming)> {
    if envelope.kind != "slash_commands" {
        return None;
    }
    let command = commands::parse(envelope.payload.get("command")?.as_str()?)?;
    let channel_id = envelope.payload.get("channel_id")?.as_str()?.to_owned();
    let sender_id = envelope.payload.get("user_id")?.as_str()?.to_owned();
    Some((
        command,
        Incoming {
            team_id: envelope
                .payload
                .get("team_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            kind: if channel_id.starts_with('D') {
                ConversationKind::DirectMessage
            } else {
                ConversationKind::Channel
            },
            channel_id,
            sender_id,
            text: String::new(),
            ts: String::new(),
            // Slash text selects session scope; use thread_ts only when no
            // explicit override was supplied.
            thread_ts: envelope
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .or_else(|| {
                    envelope
                        .payload
                        .get("thread_ts")
                        .and_then(serde_json::Value::as_str)
                })
                .map(str::to_owned),
            files: Vec::new(),
            is_reaction: false,
        },
    ))
}

fn incoming(envelope: &Envelope) -> Option<Incoming> {
    if envelope.kind != "events_api" {
        return None;
    }
    let payload: EventPayload = serde_json::from_value(envelope.payload.clone()).ok()?;
    let event = payload.event;
    let event_type = event.get("type")?.as_str()?;
    match event_type {
        "message" | "app_mention"
            if event.get("subtype").is_none() && event.get("bot_id").is_none() => {}
        "reaction_added" | "reaction_removed"
            if event.get("item")?.get("type")?.as_str()? == "message" =>
        {
            let item = event.get("item")?;
            let channel_id = item.get("channel")?.as_str()?.to_owned();
            let sender_id = event.get("user")?.as_str()?.to_owned();
            let ts = item.get("ts")?.as_str()?.to_owned();
            let reaction = event.get("reaction")?.as_str()?;
            let action = if event_type == "reaction_added" {
                "reacted with"
            } else {
                "removed reaction"
            };
            return Some(Incoming {
                team_id: payload.team_id,
                kind: if channel_id.starts_with('D') {
                    ConversationKind::DirectMessage
                } else {
                    ConversationKind::Channel
                },
                channel_id,
                sender_id: sender_id.clone(),
                text: format!("[SYSTEM] User {sender_id} {action} {reaction} on message {ts}"),
                ts,
                thread_ts: item
                    .get("thread_ts")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                files: Vec::new(),
                is_reaction: true,
            });
        }
        _ => return None,
    }
    let channel_id = event.get("channel")?.as_str()?.to_owned();
    let sender_id = event.get("user")?.as_str()?.to_owned();
    let ts = event.get("ts")?.as_str()?.to_owned();
    Some(Incoming {
        team_id: payload.team_id,
        kind: if channel_id.starts_with('D') {
            ConversationKind::DirectMessage
        } else {
            ConversationKind::Channel
        },
        channel_id,
        sender_id,
        text: event
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        ts,
        thread_ts: event
            .get("thread_ts")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        files: serde_json::from_value(event.get("files").cloned().unwrap_or_default())
            .unwrap_or_default(),
        is_reaction: false,
    })
}

async fn resolve_reaction_thread(client: &SlackClient, incoming: &mut Incoming) {
    if incoming.is_reaction && incoming.thread_ts.is_none() {
        incoming.thread_ts = client
            .message_thread_ts(&incoming.channel_id, &incoming.ts)
            .await
            .ok()
            .flatten();
    }
}

fn command_allowed(cfg: &SlackConfig, incoming: &Incoming) -> bool {
    match incoming.kind {
        ConversationKind::DirectMessage => {
            cfg.dm.enabled
                && (cfg.dm.users.is_empty()
                    || cfg.dm.users.iter().any(|user| user == &incoming.sender_id))
        }
        ConversationKind::Channel => {
            cfg.channels.is_empty()
                || cfg
                    .channels
                    .get(&incoming.channel_id)
                    .is_some_and(|channel| {
                        channel.users.is_empty()
                            || channel.users.iter().any(|user| user == &incoming.sender_id)
                    })
        }
    }
}

async fn handle_command(
    env: &WorkerEnv<'_>,
    sessions: &mut HashMap<String, ChatConn>,
    command: Command,
    incoming: Incoming,
) {
    if !command_allowed(env.cfg, &incoming) {
        return;
    }
    let message = inbound(&incoming, &env.identity.user_id);
    let key = ConversationKey::from_message(&message);
    match command {
        Command::Stop => {
            if let Some(connection) = sessions.get_mut(key.as_str()) {
                let _ = connection.session.abort().await;
            }
        }
        Command::New => reset_session(env.ctx, sessions, &key, env.data_dir).await,
        Command::Help | Command::Ping => {}
    }
    let _ = env.client; // slash response already sent as ephemeral Socket Mode ack.
}

async fn handle_message(
    env: &WorkerEnv<'_>,
    sessions: &mut HashMap<String, ChatConn>,
    control_rx: &mut mpsc::Receiver<Control>,
    incoming: Incoming,
) {
    if incoming.team_id != env.identity.team_id {
        return;
    }
    let reaction_text = incoming
        .is_reaction
        .then(|| format!("<@{}> {}", env.identity.user_id, incoming.text));
    let message = InboundMessage {
        text: reaction_text.as_deref().unwrap_or(&incoming.text),
        ..inbound(&incoming, &env.identity.user_id)
    };
    // Parse controls before routing, after removing bot mention. This lets
    // `<@bot> !stop` work in mention-gated channels without sending command
    // text to model.
    let (_, command_text) =
        crate::addressing::strip_mention(&incoming.text, Some(&env.identity.user_id), env.patterns);
    let command = command_text
        .trim_start()
        .strip_prefix('!')
        .and_then(|_| commands::parse(&command_text));
    let RouteDecision::Dispatch {
        text,
        reply_thread_ts,
    } = route(env.cfg, env.patterns, &message)
    else {
        return;
    };
    // Reply placement is presentation policy. Session scope remains tied to
    // inbound channel/thread location, so `always` cannot merge root sessions
    // into reply threads.
    let key = ConversationKey::from_message(&message);
    let key_string = key.as_str().to_owned();
    // Bang commands are message-scoped controls: unlike slash commands they
    // retain thread identity through `ConversationKey`.
    if let Some(command) = command {
        match command {
            Command::Stop => {
                if let Some(connection) = sessions.get_mut(&key_string) {
                    let _ = connection.session.abort().await;
                }
            }
            Command::New => reset_session(env.ctx, sessions, &key, env.data_dir).await,
            Command::Help | Command::Ping => {}
        }
        let _ = env
            .client
            .post_message(
                &incoming.channel_id,
                commands::reply(command),
                reply_thread_ts.as_deref(),
            )
            .await;
        return;
    }
    if !env
        .history
        .add(&key_string, incoming.ts.clone(), incoming.text.clone())
    {
        finish_reaction(env.client, env.cfg, &incoming).await;
        return;
    }
    let prompt_text = attachment_prompt(env.client, env.ctx.paths.root(), &incoming, text).await;
    // Existing backend sessions retain prior turns. Only seed a newly opened
    // session with Slack history; repeating it every turn bloats context.
    let prompt = if sessions.contains_key(&key_string) {
        prompt_text
    } else {
        env.history
            .prompt(&key_string, &prompt_text, env.cfg.history_limit)
    };
    if !sessions.contains_key(&key_string) {
        let session_dir = match current_session_dir(&key, env.data_dir) {
            Ok(session_dir) => session_dir,
            Err(_) => {
                finish_reaction(env.client, env.cfg, &incoming).await;
                let _ = env
                    .client
                    .post_message(
                        &incoming.channel_id,
                        "Could not start agent session.",
                        reply_thread_ts.as_deref(),
                    )
                    .await;
                return;
            }
        };
        let connection = match std::fs::create_dir_all(&session_dir) {
            Ok(()) => open_session(env.ctx, &session_dir).await,
            Err(error) => Err(error.into()),
        };
        match connection {
            Ok(connection) => {
                sessions.insert(key_string.clone(), connection);
            }
            Err(_) => {
                finish_reaction(env.client, env.cfg, &incoming).await;
                let _ = env
                    .client
                    .post_message(
                        &incoming.channel_id,
                        "Could not start agent session.",
                        reply_thread_ts.as_deref(),
                    )
                    .await;
                return;
            }
        }
    }
    let (mut answer, artifacts, closed, agent_succeeded, live_displayed, live_succeeded) =
        match run_turn(
            sessions.get_mut(&key_string).expect("session inserted"),
            prompt,
            &mut env.ctx.shutdown.clone(),
            control_rx,
            TurnDisplay {
                client: env.client.clone(),
                channel: incoming.channel_id.clone(),
                thread_ts: reply_thread_ts.clone(),
                show_thinking: env.cfg.show_thinking,
                delete_thinking_on_complete: env.cfg.delete_thinking_on_complete,
            },
        )
        .await
        {
            Ok(result) => result,
            Err(_) => (
                "Agent response failed.".into(),
                Vec::new(),
                true,
                false,
                false,
                true,
            ),
        };
    if answer.trim().is_empty() {
        answer = "Agent completed without a text response.".into();
    }
    if closed {
        sessions.remove(&key_string);
    }
    // A live display already represents this answer. A live Slack failure is a
    // failed reply even if the fallback post below later succeeds, so artifact
    // delivery and history clearing keep their existing all-or-nothing gate.
    let mut reply_succeeded = live_succeeded;
    if !live_displayed {
        for chunk in mrkdwn::chunk(&mrkdwn::render(&answer), 3900) {
            if env
                .client
                .post_message(&incoming.channel_id, &chunk, reply_thread_ts.as_deref())
                .await
                .is_err()
            {
                reply_succeeded = false;
            }
        }
    }
    if agent_succeeded && reply_succeeded {
        deliver_artifacts(
            env.ctx,
            env.client,
            &artifacts,
            &incoming.channel_id,
            reply_thread_ts.as_deref(),
        )
        .await;
        if env.cfg.clear_history_after_reply {
            env.history.clear(&key_string);
        }
    }
    finish_reaction(env.client, env.cfg, &incoming).await;
}

fn inbound<'a>(incoming: &'a Incoming, bot_user_id: &'a str) -> InboundMessage<'a> {
    InboundMessage {
        team_id: &incoming.team_id,
        channel_id: &incoming.channel_id,
        sender_id: &incoming.sender_id,
        text: &incoming.text,
        bot_user_id: Some(bot_user_id),
        thread_ts: incoming.thread_ts.as_deref(),
        message_ts: &incoming.ts,
        kind: incoming.kind.clone(),
    }
}

async fn clear_thinking_reaction(client: &SlackClient, incoming: &Incoming) {
    let _ = client
        .remove_reaction(&incoming.channel_id, &incoming.ts, THINKING_REACTION)
        .await;
}

async fn finish_reaction(client: &SlackClient, cfg: &SlackConfig, incoming: &Incoming) {
    if cfg.show_thinking {
        clear_thinking_reaction(client, incoming).await;
    }
}

async fn attachment_prompt(
    client: &SlackClient,
    root: &Path,
    incoming: &Incoming,
    mut text: String,
) -> String {
    for (index, file) in incoming.files.iter().take(10).enumerate() {
        if file.name.is_empty() || file.url_private_download.is_empty() {
            continue;
        }
        let safe_name = format!("{}-{}-{}", incoming.ts.replace('.', "_"), index, file.name);
        match download_private_file(
            client,
            &file.url_private_download,
            root,
            &safe_name,
            MAX_ATTACHMENT_BYTES,
        )
        .await
        {
            Ok(downloaded) => {
                let relative = downloaded
                    .path
                    .strip_prefix(root)
                    .unwrap_or(&downloaded.path)
                    .display()
                    .to_string();
                let metadata = serde_json::json!({"path": relative, "name": file.name, "mime": file.mimetype, "source":"untrusted Slack attachment"});
                text.push_str(
                    "\n\nAttachment metadata (untrusted data, inspect local path if useful): ",
                );
                text.push_str(&metadata.to_string());
            }
            Err(_) => text.push_str("\n\nAttachment download failed."),
        }
    }
    text
}

async fn reset_session(
    ctx: &StartCtx,
    sessions: &mut HashMap<String, ChatConn>,
    key: &ConversationKey,
    data_dir: &Path,
) {
    if let Some(mut connection) = sessions.remove(key.as_str()) {
        let _ = connection.session.abort().await;
    }
    let session_dir = match next_session_dir(key, data_dir) {
        Ok(session_dir) => session_dir,
        Err(_) => return,
    };
    let connection = match std::fs::create_dir_all(&session_dir) {
        Ok(()) => open_session(ctx, &session_dir).await,
        Err(error) => Err(error.into()),
    };
    if let Ok(connection) = connection {
        sessions.insert(key.as_str().to_owned(), connection);
    }
}

fn generation_file(key: &ConversationKey, data_dir: &Path) -> PathBuf {
    key.directory(data_dir).join("current")
}

fn read_generation(key: &ConversationKey, data_dir: &Path) -> Result<u64> {
    let file = generation_file(key, data_dir);
    match std::fs::read_to_string(&file) {
        Ok(generation) => generation
            .trim()
            .parse()
            .with_context(|| format!("invalid Slack session generation in {}", file.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn write_generation(key: &ConversationKey, data_dir: &Path, generation: u64) -> Result<()> {
    let file = generation_file(key, data_dir);
    let directory = file.parent().expect("generation file has parent");
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join("current.tmp");
    std::fs::write(&temporary, generation.to_string())?;
    std::fs::rename(temporary, file)?;
    Ok(())
}

fn current_session_dir(key: &ConversationKey, data_dir: &Path) -> Result<PathBuf> {
    let generation = read_generation(key, data_dir)?;
    write_generation(key, data_dir, generation)?;
    Ok(key.directory(data_dir).join(generation.to_string()))
}

fn next_session_dir(key: &ConversationKey, data_dir: &Path) -> Result<PathBuf> {
    let generation = read_generation(key, data_dir)?
        .checked_add(1)
        .context("Slack session generation overflow")?;
    write_generation(key, data_dir, generation)?;
    Ok(key.directory(data_dir).join(generation.to_string()))
}

async fn open_session(ctx: &StartCtx, session_dir: &Path) -> Result<ChatConn> {
    let backend_id = dar_extension_sdk::chat::resolve_agent_backend(ctx, None);
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
    let (artifact_tx, artifacts) = mpsc::channel(32);
    let params = dar_extension_sdk::chat::agent_session_params(ctx, session_dir)
        .artifact_ready(Some(artifact_tx))
        .build();
    let (tx, rx) = mpsc::channel(256);
    Ok(ChatConn {
        session: backend.open(params, tx).await?,
        rx,
        artifacts,
    })
}

async fn run_turn(
    conn: &mut ChatConn,
    prompt: String,
    shutdown: &mut dar_extension_sdk::ShutdownToken,
    control_rx: &mut mpsc::Receiver<Control>,
    display: TurnDisplay,
) -> Result<(String, Vec<ArtifactReady>, bool, bool, bool, bool)> {
    let thinking = display.show_thinking.then(|| {
        Thinking::start(
            display.client.clone(),
            display.channel.clone(),
            display.thread_ts.clone(),
            display.delete_thinking_on_complete,
        )
    });
    let mut live_answer = LiveAnswer::new(
        display.client.clone(),
        display.channel.clone(),
        display.thread_ts.clone(),
    );
    if let Err(error) = conn.session.send_turn(prompt).await {
        if let Some(thinking) = thinking {
            thinking.finish().await;
        }
        return Err(error);
    }
    let mut answer = String::new();
    let mut artifacts = Vec::new();
    let result = loop {
        tokio::select! {
            _ = live_answer.wait_for_flush() => {
                live_answer.flush_if_due(&answer).await;
            }
            _ = shutdown.cancelled() => {
                let _ = conn.session.abort().await;
                break (answer, true, false);
            }
            control = control_rx.recv() => match control {
                Some(Control::Stop(reply)) => {
                    let _ = reply.send(conn.session.abort().await.is_ok());
                }
                None => {}
            },
            artifact = conn.artifacts.recv() => if let Some(artifact) = artifact {
                append_artifact(&mut artifacts, artifact);
            },
            event = conn.rx.recv() => match event {
                Some(ChatEvent::Delta { role: ChatRole::Assistant, text }) => {
                    answer.push_str(&text);
                    live_answer.push(&answer).await;
                }
                Some(ChatEvent::Delta { role: ChatRole::Thinking, text }) => {
                    if let Some(thinking) = &thinking { thinking.append(text); }
                }
                Some(ChatEvent::TurnFinished { ok, error }) => {
                    if !ok && answer.is_empty() {
                        answer = format!("Agent turn failed: {}", error.unwrap_or_else(|| "unknown error".into()));
                    }
                    break (answer, false, ok);
                }
                Some(ChatEvent::SessionClosed { .. }) | None => break (answer, true, false),
                _ => {}
            }
        }
    };
    while let Ok(artifact) = conn.artifacts.try_recv() {
        append_artifact(&mut artifacts, artifact);
    }
    if let Some(thinking) = thinking {
        thinking.finish().await;
    }
    let (live_displayed, live_succeeded) = live_answer.finish(&result.0).await;
    Ok((
        result.0,
        artifacts,
        result.1,
        result.2,
        live_displayed,
        live_succeeded,
    ))
}

fn append_artifact(artifacts: &mut Vec<ArtifactReady>, artifact: ArtifactReady) {
    if !artifacts.iter().any(|existing| existing.id == artifact.id) {
        artifacts.push(artifact);
    }
}

fn slack_delivery_target(channel: &str, thread_ts: Option<&str>) -> ArtifactDeliveryTarget {
    ArtifactDeliveryTarget {
        surface_id: "slack".into(),
        origin_destination: serde_json::json!({
            "channel": channel,
            "thread_ts": thread_ts,
        })
        .to_string(),
    }
}

fn verified_artifact_upload(
    store: &dar_extension_sdk::artifacts::ArtifactStore,
    id: dar_extension_sdk::artifacts::ArtifactId,
) -> Result<(String, Vec<u8>)> {
    let mut verified = store.open_verified(id)?;
    let filename = verified.metadata().filename.clone();
    let mut bytes = Vec::with_capacity(verified.metadata().bytes as usize);
    verified.read_to_end(&mut bytes)?;
    Ok((filename, bytes))
}

async fn deliver_artifacts(
    ctx: &StartCtx,
    client: &SlackClient,
    artifacts: &[ArtifactReady],
    channel: &str,
    thread_ts: Option<&str>,
) {
    let Ok(store) = ctx
        .host
        .services
        .get_named::<dar_extension_sdk::artifacts::ArtifactStore>("artifact-store")
    else {
        return;
    };
    let target = slack_delivery_target(channel, thread_ts);
    for artifact in artifacts {
        if artifact.validate(&store).is_none() {
            continue;
        }
        let claim = match store.claim_delivery(artifact.id, target.clone()) {
            Ok(DeliveryClaimResult::Claimed(claim)) => claim,
            Ok(DeliveryClaimResult::Delivered(_) | DeliveryClaimResult::InProgress) | Err(_) => {
                continue
            }
        };
        let uploaded = verified_artifact_upload(&store, artifact.id);
        let Ok((filename, bytes)) = uploaded else {
            let _ = store.release_delivery(claim);
            continue;
        };
        match client
            .upload_bytes(&filename, bytes, channel, thread_ts)
            .await
        {
            Ok(remote_id) => {
                // Keep claim if receipt write fails: upload happened, so releasing it
                // would permit a duplicate upload on retry.
                let _ = store.complete_delivery(claim, remote_id);
            }
            Err(_) => {
                let _ = store.release_delivery(claim);
                let _ = client
                    .post_message(channel, "Generated file could not be uploaded.", thread_ts)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn delivery_test_artifact() -> (
        std::path::PathBuf,
        dar_extension_sdk::artifacts::ArtifactStore,
        dar_extension_sdk::artifacts::ArtifactMetadata,
    ) {
        use dar_extension_sdk::artifacts::{ArtifactMetadataInput, ExportRoot};

        let root = std::env::temp_dir().join(format!(
            "slack-artifact-delivery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let export_dir = root.join("export");
        std::fs::create_dir_all(&export_dir).unwrap();
        std::fs::write(export_dir.join("artifact.txt"), "artifact bytes").unwrap();
        let store =
            dar_extension_sdk::artifacts::ArtifactStore::open(root.join("vault"), 1024).unwrap();
        let metadata = store
            .stage_from_export_root(
                &ExportRoot::open(&export_dir).unwrap(),
                "artifact.txt",
                ArtifactMetadataInput {
                    filename: "verified-name.txt".into(),
                    media_type: Some("text/plain".into()),
                    caption: None,
                },
            )
            .unwrap();
        (root, store, metadata)
    }

    #[test]
    fn artifact_delivery_target_includes_exact_channel_and_thread() {
        let target = slack_delivery_target("C123", Some("1234.5678"));
        assert_eq!(target.surface_id, "slack");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&target.origin_destination).unwrap(),
            serde_json::json!({"channel":"C123","thread_ts":"1234.5678"})
        );
        assert_ne!(target, slack_delivery_target("C123", Some("9999.0000")));
        assert_ne!(target, slack_delivery_target("C123", None));
    }

    #[test]
    fn verified_vault_filename_overrides_artifact_event_name() {
        let (root, store, artifact) = delivery_test_artifact();
        let (filename, bytes) = verified_artifact_upload(&store, artifact.id).unwrap();
        assert_eq!(filename, "verified-name.txt");
        assert_eq!(bytes, b"artifact bytes");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn artifact_delivery_claim_prevents_duplicates_and_releases_failed_uploads() {
        let (root, store, artifact) = delivery_test_artifact();
        let target = slack_delivery_target("C123", Some("1234.5678"));
        let claim = match store.claim_delivery(artifact.id, target.clone()).unwrap() {
            DeliveryClaimResult::Claimed(claim) => claim,
            state => panic!("unexpected delivery state: {state:?}"),
        };
        assert!(matches!(
            store.claim_delivery(artifact.id, target.clone()).unwrap(),
            DeliveryClaimResult::InProgress
        ));

        store.release_delivery(claim).unwrap();
        let retry = match store.claim_delivery(artifact.id, target.clone()).unwrap() {
            DeliveryClaimResult::Claimed(claim) => claim,
            state => panic!("unexpected retry state: {state:?}"),
        };
        store.complete_delivery(retry, "F123".into()).unwrap();
        assert!(matches!(
            store.claim_delivery(artifact.id, target).unwrap(),
            DeliveryClaimResult::Delivered(_)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn artifact_events_preserve_order_and_dedupe_id() {
        let value = serde_json::json!({"type":"resource_link","uri":"dar-artifact://00000000-0000-4000-8000-000000000001","name":"one.txt","bytes":1,"sha256":"a"});
        let first = ArtifactReady::from_publish_resource("artifact_publish", &value).unwrap();
        let mut value = value;
        value["name"] = serde_json::Value::String("duplicate.txt".into());
        let duplicate = ArtifactReady::from_publish_resource("artifact_publish", &value).unwrap();
        let mut artifacts = Vec::new();
        append_artifact(&mut artifacts, first);
        append_artifact(&mut artifacts, duplicate);
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].name, "one.txt");
    }

    #[test]
    fn ack_with_ephemeral_payload() {
        assert!(ack("x", Some("ok")).contains("ephemeral"));
    }
    #[test]
    fn socket_mode_hello_logs_once_per_connection() {
        let hello: Envelope = serde_json::from_value(serde_json::json!({"type":"hello"})).unwrap();
        let other: Envelope =
            serde_json::from_value(serde_json::json!({"type":"disconnect"})).unwrap();
        let mut hello_logged = false;

        assert!(should_log_socket_mode_hello(&hello, &mut hello_logged));
        assert!(!should_log_socket_mode_hello(&hello, &mut hello_logged));
        assert!(!should_log_socket_mode_hello(&other, &mut hello_logged));
    }

    #[test]
    fn dedupe_prefers_event_id_over_socket_envelope() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "envelope_id":"socket-retry", "type":"events_api",
            "payload":{"event_id":"Ev1", "team_id":"T1", "event":{}}
        }))
        .unwrap();
        assert_eq!(dedupe_key(&envelope).as_deref(), Some("event:Ev1"));
    }

    #[test]
    fn dedupe_is_bounded() {
        let mut dedupe = HashMap::new();
        for index in 0..=MAX_DEDUPE_ENTRIES {
            dedupe.insert(index.to_string(), Instant::now());
        }
        trim_dedupe(&mut dedupe);
        assert_eq!(dedupe.len(), MAX_DEDUPE_ENTRIES);
    }

    #[test]
    fn standard_event_uses_payload_team_id() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type":"events_api", "payload":{"event_id":"Ev1", "team_id":"T1",
            "event":{"type":"message","channel":"D1","user":"U1","ts":"1.2","text":"hello"}}
        }))
        .unwrap();
        let event = incoming(&envelope).expect("standard Events API message routes");
        assert_eq!(event.team_id, "T1");
    }
    #[test]
    fn filters_bot_messages_and_reads_files() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({"type":"events_api","payload":{"event":{"type":"message","channel":"D1","user":"U1","ts":"1.2","files":[{"name":"x.png","mimetype":"image/png","url_private_download":"https://files.slack.com/x"}]}}})).unwrap();
        assert_eq!(incoming(&envelope).unwrap().files.len(), 1);
    }

    #[test]
    fn reaction_added_without_thread_ts_keeps_target_for_api_lookup() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type":"events_api", "payload":{"team_id":"T1", "event":{
                "type":"reaction_added", "user":"U1", "reaction":"eyes",
                "item":{"type":"message", "channel":"C1", "ts":"1.2"}
            }}
        }))
        .unwrap();
        let event = incoming(&envelope).expect("message reaction routes");
        assert_eq!(
            event.text,
            "[SYSTEM] User U1 reacted with eyes on message 1.2"
        );
        assert_eq!(event.thread_ts, None);
        assert!(event.is_reaction);
        let mentioned = format!("<@B1> {}", event.text);
        assert!(matches!(
            route(
                &SlackConfig::default(),
                &[],
                &InboundMessage {
                    text: &mentioned,
                    ..inbound(&event, "B1")
                }
            ),
            RouteDecision::Dispatch { .. }
        ));
    }

    #[test]
    fn reaction_removed_routes_and_rejects_non_message_items() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type":"events_api", "payload":{"event":{
                "type":"reaction_removed", "user":"U1", "reaction":"eyes",
                "item":{"type":"message", "channel":"C1", "ts":"1.2"}
            }}
        }))
        .unwrap();
        assert_eq!(
            incoming(&envelope).unwrap().text,
            "[SYSTEM] User U1 removed reaction eyes on message 1.2"
        );
        let file_reaction: Envelope = serde_json::from_value(serde_json::json!({
            "type":"events_api", "payload":{"event":{
                "type":"reaction_added", "user":"U1", "reaction":"eyes",
                "item":{"type":"file", "channel":"C1", "ts":"1.2"}
            }}
        }))
        .unwrap();
        assert!(incoming(&file_reaction).is_none());
    }
    #[test]
    fn generation_mapping_rotates_without_removing_prior_session() {
        let data_dir =
            std::env::temp_dir().join(format!("slack-runtime-generation-{}", std::process::id()));
        let message = InboundMessage {
            team_id: "T1",
            channel_id: "C1",
            sender_id: "U1",
            text: "hello",
            bot_user_id: Some("B1"),
            thread_ts: None,
            message_ts: "1.1",
            kind: ConversationKind::Channel,
        };
        let key = ConversationKey::from_message(&message);
        let first = current_session_dir(&key, &data_dir).unwrap();
        std::fs::create_dir_all(&first).unwrap();
        std::fs::write(first.join("marker"), "old").unwrap();

        let second = next_session_dir(&key, &data_dir).unwrap();

        assert_ne!(first, second);
        assert_eq!(
            std::fs::read_to_string(generation_file(&key, &data_dir)).unwrap(),
            "1"
        );
        assert_eq!(
            std::fs::read_to_string(first.join("marker")).unwrap(),
            "old"
        );
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn keyed_dispatch_keeps_scoped_controls_in_message_lane() {
        let incoming = Incoming {
            team_id: "T".into(),
            channel_id: "C".into(),
            sender_id: "U".into(),
            text: "!stop".into(),
            ts: "2.0".into(),
            thread_ts: Some("1.0".into()),
            kind: ConversationKind::Channel,
            files: vec![],
            is_reaction: false,
        };
        let message_key = work_key(&Work::Message(incoming.clone()));
        let command_key = work_key(&Work::Command {
            command: Command::Stop,
            incoming: incoming.clone(),
        });
        assert_eq!(message_key, command_key);

        let other_thread = Work::Message(Incoming {
            thread_ts: Some("3.0".into()),
            ..incoming
        });
        assert_ne!(message_key, work_key(&other_thread));
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn stop_control_targets_active_same_key_without_work_queue() {
        let incoming = Incoming {
            team_id: "T".into(),
            channel_id: "C".into(),
            sender_id: "U".into(),
            text: "!stop".into(),
            ts: "2.0".into(),
            thread_ts: Some("1.0".into()),
            kind: ConversationKind::Channel,
            files: vec![],
            is_reaction: false,
        };
        let key = ConversationKey::from_message(&inbound(&incoming, ""))
            .as_str()
            .to_owned();
        let (work, _work_rx) = mpsc::channel(1);
        let (control, mut control_rx) = mpsc::channel(1);
        let mut workers = HashMap::from([(key, Worker { work, control })]);
        let responder = tokio::spawn(async move {
            let Some(Control::Stop(reply)) = control_rx.recv().await else {
                panic!("expected stop control");
            };
            reply.send(true).unwrap();
        });

        assert!(stop_active_session(&mut workers, &incoming).await);
        responder.await.unwrap();
    }

    #[test]
    fn bang_command_strips_bot_mention_before_routing() {
        let incoming = Incoming {
            team_id: "T".into(),
            channel_id: "C".into(),
            sender_id: "U".into(),
            text: "<@B1> !stop".into(),
            ts: "1.0".into(),
            thread_ts: Some("0.1".into()),
            kind: ConversationKind::Channel,
            files: vec![],
            is_reaction: false,
        };
        assert_eq!(
            message_command(
                &incoming,
                &SlackIdentity {
                    user_id: "B1".into(),
                    team_id: "T".into(),
                    bot_id: None,
                },
                &[]
            ),
            Some(Command::Stop)
        );
    }

    #[test]
    fn accepted_message_log_uses_only_safe_identifiers() {
        let incoming = Incoming {
            team_id: "T1".into(),
            channel_id: "D1\nsecret".into(),
            sender_id: "U1\tuser".into(),
            text: "message content must not appear".into(),
            ts: "1.0".into(),
            thread_ts: None,
            kind: ConversationKind::DirectMessage,
            files: vec![],
            is_reaction: false,
        };
        assert_eq!(
            accepted_message_log(&incoming),
            "message from channel D1secret (user U1user)"
        );
    }

    #[test]
    fn accepted_message_requires_matching_team_and_route() {
        let mut cfg = SlackConfig::default();
        cfg.dm.enabled = true;
        let identity = SlackIdentity {
            user_id: "B1".into(),
            team_id: "T1".into(),
            bot_id: None,
        };
        let incoming = Incoming {
            team_id: "T1".into(),
            channel_id: "D1".into(),
            sender_id: "U1".into(),
            text: "hello".into(),
            ts: "1.0".into(),
            thread_ts: None,
            kind: ConversationKind::DirectMessage,
            files: vec![],
            is_reaction: false,
        };
        assert!(accepted_message(&cfg, &identity, &[], &incoming));
        assert!(!accepted_message(
            &cfg,
            &SlackIdentity {
                team_id: "T2".into(),
                ..identity
            },
            &[],
            &incoming,
        ));
    }

    #[test]
    fn slash_command_preserves_thread_scope() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "slash_commands",
            "payload": {
                "command": "/stop",
                "team_id": "T1",
                "channel_id": "C1",
                "user_id": "U1",
                "thread_ts": "1.2"
            }
        }))
        .unwrap();

        let (command, incoming) = slash_command(&envelope).expect("slash command parses");
        assert_eq!(command, Command::Stop);
        assert_eq!(incoming.thread_ts.as_deref(), Some("1.2"));
        assert_eq!(
            work_key(&Work::Message(Incoming {
                text: "running".into(),
                ts: "1.3".into(),
                files: vec![],
                is_reaction: false,
                ..incoming.clone()
            })),
            work_key(&Work::Command { command, incoming })
        );
    }

    #[test]
    fn slash_command_text_overrides_session_scope() {
        let envelope: Envelope = serde_json::from_value(serde_json::json!({
            "type": "slash_commands",
            "payload": {
                "command": "/new", "team_id": "T1", "channel_id": "C1",
                "user_id": "U1", "thread_ts": "1.2", "text": "project-alpha"
            }
        }))
        .unwrap();
        let (_, incoming) = slash_command(&envelope).unwrap();
        assert_eq!(incoming.thread_ts.as_deref(), Some("project-alpha"));
    }

    #[test]
    fn command_policy_respects_dm_allowlist() {
        let cfg = SlackConfig::default();
        let incoming = Incoming {
            team_id: "T".into(),
            channel_id: "D".into(),
            sender_id: "U".into(),
            text: String::new(),
            ts: String::new(),
            thread_ts: None,
            kind: ConversationKind::DirectMessage,
            files: vec![],
            is_reaction: false,
        };
        assert!(!command_allowed(&cfg, &incoming));
    }
}
