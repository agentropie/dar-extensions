use anyhow::{Context, Result};
use dar_extension_sdk::{
    chat::{AgentSender, ChatBackend, ChatEvent, ChatRole},
    StartCtx,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    addressing, attachments, commands, config, delivery, history::History, live_answer, session,
    Gateway,
};

struct ActiveTurn {
    id: u64,
    cancel: CancellationToken,
    done: oneshot::Receiver<()>,
}

#[derive(Default)]
struct Threads {
    parents: HashMap<String, String>,
}
impl ActiveTurn {
    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.done.await;
    }
}

struct ConnectionEnv<'a> {
    ctx: &'a StartCtx,
    cfg: &'a config::DiscordConfig,
    token: &'a str,
    data: &'a Path,
    root: &'a Path,
    client: &'a reqwest::Client,
    turns: &'a Arc<Mutex<HashMap<session::SessionKey, ActiveTurn>>>,
    threads: &'a Arc<Mutex<Threads>>,
    history: &'a Arc<History>,
    next_turn: &'a AtomicU64,
    guards: &'a Guards,
}

/// Loop guard per conversation. Each Discord turn opens a fresh chat
/// session, so the backend's per-session guard never accumulates; keep one
/// here that outlives sessions.
type Guards = Arc<std::sync::Mutex<HashMap<session::SessionKey, cap_chat::LoopGuard>>>;

pub async fn run(
    ctx: StartCtx,
    cfg: config::DiscordConfig,
    token: String,
    data: std::path::PathBuf,
) -> Result<()> {
    let client = reqwest::Client::new();
    let root = ctx.paths.root().to_path_buf();
    let turns = Arc::new(Mutex::new(HashMap::new()));
    let threads = Arc::new(Mutex::new(Threads::default()));
    let history = Arc::new(History::default());
    let next_turn = AtomicU64::new(0);
    let guards = Guards::default();
    let mut delay = Duration::from_secs(1);
    loop {
        if ctx.shutdown.is_cancelled() {
            stop_turns(&turns).await;
            return Ok(());
        }
        let mut shutdown = ctx.shutdown.clone();
        let gateway = match tokio::select! {
            _ = shutdown.cancelled() => { stop_turns(&turns).await; return Ok(()); }
            result = gateway_url(&client, &token) => result,
        } {
            Ok(url) => url,
            Err(error) => {
                tracing::warn!(%error, "discord gateway discovery failed; retrying");
                let mut shutdown = ctx.shutdown.clone();
                wait_or_shutdown(&mut shutdown, delay).await;
                delay = reconnect_delay(delay);
                continue;
            }
        };
        let mut shutdown = ctx.shutdown.clone();
        let socket = match tokio::select! {
            _ = shutdown.cancelled() => { stop_turns(&turns).await; return Ok(()); }
            result = tokio_tungstenite::connect_async(format!("{}/?v=10&encoding=json", gateway.trim_end_matches('/'))) => result,
        } {
            Ok((socket, _)) => socket,
            Err(error) => {
                tracing::warn!(%error, "discord gateway connection failed; retrying");
                let mut shutdown = ctx.shutdown.clone();
                wait_or_shutdown(&mut shutdown, delay).await;
                delay = reconnect_delay(delay);
                continue;
            }
        };
        delay = Duration::from_secs(1);
        if let Err(error) = run_connection(
            ConnectionEnv {
                ctx: &ctx,
                cfg: &cfg,
                token: &token,
                data: &data,
                root: &root,
                client: &client,
                turns: &turns,
                threads: &threads,
                history: &history,
                next_turn: &next_turn,
                guards: &guards,
            },
            socket,
        )
        .await
        {
            tracing::warn!(%error, "discord gateway disconnected; reconnecting");
        }
        if ctx.shutdown.is_cancelled() {
            stop_turns(&turns).await;
            return Ok(());
        }
        let mut shutdown = ctx.shutdown.clone();
        wait_or_shutdown(&mut shutdown, delay).await;
        delay = reconnect_delay(delay);
    }
}

async fn gateway_url(client: &reqwest::Client, token: &str) -> Result<String> {
    Ok(client
        .get("https://discord.com/api/v10/gateway/bot")
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await?
        .error_for_status()?
        .json::<Gateway>()
        .await?
        .url)
}

async fn run_connection(
    env: ConnectionEnv<'_>,
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<()> {
    let (mut write, mut read) = socket.split();
    let mut shutdown = env.ctx.shutdown.clone();
    let hello = tokio::select! {
        _ = shutdown.cancelled() => {
            let _ = close_gateway(&mut write).await;
            return Ok(());
        }
        result = next_json(&mut read) => result?,
    };
    let interval = hello["d"]["heartbeat_interval"]
        .as_u64()
        .context("Discord gateway hello missing heartbeat_interval")?;
    write.send(Message::Text(json!({"op":2,"d":{"token":env.token,"intents":37377,"properties":{"os":"dar","browser":"dar","device":"dar"}}}).to_string())).await?;
    let mut heartbeat = tokio::time::interval(Duration::from_millis(interval));
    let mut sequence = None;
    let mut bot_user_id = None;
    dar_extension_sdk::log::event("-", "discord", "gateway connected");
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = close_gateway(&mut write).await;
                return Ok(());
            },
            _ = heartbeat.tick() => write.send(Message::Text(json!({"op":1,"d":sequence}).to_string())).await?,
            message = read.next() => {
                let Some(message) = message else { anyhow::bail!("Discord gateway closed") };
                let Some(value) = parse_message(message?)? else { continue };
                if gateway_requests_reconnect(&value) { anyhow::bail!("Discord gateway requested reconnect"); }
                if let Some(seq) = value["s"].as_i64() { sequence = Some(seq); }
                if value["t"] == "READY" {
                    bot_user_id = value["d"]["user"]["id"].as_str().map(str::to_owned);
                    let name = value["d"]["user"]["username"].as_str().unwrap_or("?");
                    dar_extension_sdk::log::event("-", "discord", &format!("connected as @{name}"));
                }
                if update_thread_event(env.threads, value["t"].as_str(), &value["d"]).await {
                    continue;
                }
                if let Some("MESSAGE_CREATE") = value["t"].as_str() {
                    handle_message(&env, bot_user_id.as_deref(), &value["d"]).await;
                }
            }
        }
    }
}

async fn close_gateway<W>(write: &mut W) -> Result<()>
where
    W: futures_util::Sink<Message> + Unpin,
    W::Error: std::error::Error + Send + Sync + 'static,
{
    write.send(Message::Close(None)).await?;
    write.close().await?;
    Ok(())
}

async fn stop_turns(turns: &Arc<Mutex<HashMap<session::SessionKey, ActiveTurn>>>) {
    let active = std::mem::take(&mut *turns.lock().await);
    for (_, turn) in active {
        turn.stop().await;
    }
}

fn reconnect_delay(delay: Duration) -> Duration {
    (delay * 2).min(Duration::from_secs(30))
}

fn gateway_requests_reconnect(value: &Value) -> bool {
    matches!(value["op"].as_i64(), Some(7 | 9))
}

async fn wait_or_shutdown(shutdown: &mut dar_extension_sdk::ShutdownToken, delay: Duration) {
    tokio::select! { _ = shutdown.cancelled() => {}, _ = tokio::time::sleep(delay) => {} }
}

async fn handle_message(env: &ConnectionEnv<'_>, bot_user_id: Option<&str>, message: &Value) {
    let ctx = env.ctx;
    let cfg = env.cfg;
    let token = env.token;
    let data = env.data;
    let root = env.root;
    let client = env.client;
    let turns = env.turns;
    let threads = env.threads;
    let history = env.history;
    let next_turn = env.next_turn;
    let guards = Arc::clone(env.guards);
    let attachments = attachments::parse(message["attachments"].as_array());
    let content = message["content"].as_str().unwrap_or("");
    if let Some(thread) = message.get("thread") {
        update_thread(threads, thread).await;
    }
    let channel_id = message["channel_id"].as_str().unwrap_or("");
    let parent_channel_id = {
        let threads = threads.lock().await;
        threads.parents.get(channel_id).cloned()
    };
    let thread_session_key = message["guild_id"]
        .as_str()
        .zip(parent_channel_id.as_ref())
        .map(|(guild_id, _)| session::SessionKey::guild_thread(guild_id, channel_id));
    let thread_engaged = thread_session_key.as_ref().is_some_and(|key| {
        session::is_active_engagement(data, key, cfg.sessions.idle_minutes, session::now())
    });
    let history_key = session::history_key(
        message["guild_id"].as_str(),
        channel_id,
        parent_channel_id.as_deref(),
        message["author"]["id"].as_str(),
    );
    if let Some((message_id, history_text)) = history_entry(message, bot_user_id) {
        history.add(&history_key, message_id, history_text);
    }
    let route = addressing::route(
        cfg,
        bot_user_id,
        &addressing::InboundMessage {
            guild_id: message["guild_id"].as_str(),
            channel_id,
            parent_channel_id: parent_channel_id.as_deref(),
            thread_engaged,
            author_id: message["author"]["id"].as_str().unwrap_or(""),
            author_is_bot: message["author"]["bot"].as_bool().unwrap_or(false),
            webhook_id: message["webhook_id"].as_str(),
            text: content,
            has_attachments: !attachments.is_empty(),
            mentions_bot: bot_user_id.is_some_and(|bot| {
                message["mentions"]
                    .as_array()
                    .is_some_and(|mentions| mentions.iter().any(|m| m["id"] == bot))
            }),
        },
    );
    let addressing::RouteDecision::Dispatch { text, session_key } = route else {
        if let (Some(guild), Some(bot)) = (message["guild_id"].as_str(), bot_user_id) {
            if content.contains(&format!("<@{bot}>")) || content.contains(&format!("<@!{bot}>")) {
                dar_extension_sdk::log::event(
                    "-",
                    "discord",
                    &format!(
                        "ignored mention in guild {guild} channel {}: not configured or sender not allowed",
                        parent_channel_id.as_deref().unwrap_or(channel_id)
                    ),
                );
            }
        }
        return;
    };
    if content.trim().is_empty() && attachments.is_empty() {
        return;
    }
    let Ok(channel) = message["channel_id"]
        .as_str()
        .context("Discord message missing channel id")
    else {
        return;
    };
    let Ok(message_id) = message["id"].as_str().context("Discord message missing id") else {
        return;
    };
    let delivery =
        delivery::Delivery::new(client.clone(), token, channel, message_id, &cfg.ack_emoji);
    let author = message["author"]["id"].as_str().unwrap_or("?");
    let source = match message["guild_id"].as_str() {
        Some(guild) => format!("guild {guild} channel {channel}"),
        None => format!("DM channel {channel}"),
    };
    dar_extension_sdk::log::event(
        "-",
        "discord",
        &format!("message from {source} (user {author})"),
    );
    if let Err(error) = delivery.acknowledge().await {
        delivery.failure(&error).await;
        return;
    }
    delivery.typing().await;
    if let Some(command) = commands::parse(content) {
        if let Some(turn) = turns.lock().await.remove(&session_key) {
            turn.stop().await;
        }
        if command == commands::Command::Reset {
            if let Err(error) = session::reset_with_activity(data, &session_key, session::now()) {
                delivery.failure(&error).await;
                return;
            }
            history.clear(&history_key);
        }
        if let Err(error) = delivery.post(commands::reply(command)).await {
            delivery.failure(&error).await;
        }
        return;
    }
    if let Some(turn) = turns.lock().await.remove(&session_key) {
        turn.stop().await;
    }
    // In-memory history is empty after restart; backfill once per conversation.
    if cfg.fetch_history && history.claim_seed(&history_key) {
        match fetch_history(
            client,
            token,
            channel,
            message_id,
            cfg.history_limit,
            bot_user_id,
        )
        .await
        {
            Ok(older) => history.seed(&history_key, older),
            Err(error) => tracing::warn!(%error, "discord history fetch failed"),
        }
    }
    match session::prepare_activity(
        data,
        &session_key,
        cfg.sessions.idle_minutes,
        session::now(),
    ) {
        Ok(true) => {
            history.clear(&history_key);
            if let Err(error) = delivery.post(session::EXPIRED_NOTICE).await {
                tracing::warn!(%error, "discord expiry notice delivery failed");
            }
        }
        Ok(false) => {}
        Err(error) => {
            delivery.failure(&error).await;
            return;
        }
    }
    if parent_channel_id.is_some() {
        if let Err(error) = session::engage(data, &session_key) {
            delivery.failure(&error).await;
            return;
        }
    }
    let token = token.to_owned();
    let backend = cfg.backend.clone();
    let history = Arc::clone(history);
    let history_limit = cfg.history_limit;
    let clear_history_after_reply = cfg.clear_history_after_reply;
    let ctx = ctx.clone();
    let data = data.to_path_buf();
    let root = root.to_path_buf();
    let channel = channel.to_owned();
    let history_message_id = message_id.to_owned();
    let prompt = text;
    let sender = turn_sender(message);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let id = next_turn.fetch_add(1, Ordering::Relaxed);
    let (done_tx, done) = oneshot::channel();
    if let Some(turn) = turns.lock().await.insert(
        session_key.clone(),
        ActiveTurn {
            id,
            cancel: cancel.clone(),
            done,
        },
    ) {
        turn.stop().await;
    }
    let turns = Arc::clone(turns);
    tokio::spawn(async move {
        // Discord typing expires after ~10s and edits don't clear it, so keep
        // it alive for the whole turn; it may linger briefly after the reply.
        let typing = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                delivery.typing().await;
            }
        };
        let answered = answer(AnswerRequest {
            ctx,
            configured: backend,
            data,
            root,
            token,
            channel,
            session_key: session_key.clone(),
            history_key,
            history_message_id,
            history,
            history_limit,
            clear_history_after_reply,
            text: prompt,
            attachments,
            cancel,
            sender,
            guards,
        });
        let result = tokio::select! {
            result = answered => result,
            () = typing => unreachable!(),
        };
        match result {
            // Silent turn: nothing posted, so drop the ack too.
            Ok(true) => delivery.unacknowledge().await,
            Ok(false) => {}
            Err(error) => {
                if !task_cancel.is_cancelled() {
                    tracing::warn!(%error, "discord turn failed");
                    delivery.failure(&error).await;
                }
            }
        }
        let _ = done_tx.send(());
        let mut turns = turns.lock().await;
        if turns.get(&session_key).is_some_and(|turn| turn.id == id) {
            turns.remove(&session_key);
        }
    });
}

/// Recent human messages before `before`, oldest first, formatted like live history.
async fn fetch_history(
    client: &reqwest::Client,
    token: &str,
    channel: &str,
    before: &str,
    limit: usize,
    bot_user_id: Option<&str>,
) -> Result<Vec<(String, String)>> {
    let limit = if limit == 0 { 50 } else { limit.min(50) };
    let messages: Vec<Value> = client
        .get(format!(
            "https://discord.com/api/v10/channels/{channel}/messages?before={before}&limit={limit}"
        ))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(messages
        .iter()
        .rev()
        .filter_map(|m| history_entry(m, bot_user_id))
        .collect())
}

/// History entry for a message: humans and other bots (labelled, so the
/// agent can tell them apart); never our own bot or webhooks.
fn history_entry(message: &Value, bot_user_id: Option<&str>) -> Option<(String, String)> {
    let author = &message["author"];
    let own = bot_user_id.is_some_and(|bot| author["id"] == bot);
    if own || !message["webhook_id"].is_null() {
        return None;
    }
    let content = message["content"].as_str().unwrap_or("");
    let has_attachments = message["attachments"]
        .as_array()
        .is_some_and(|a| !a.is_empty());
    let text = match (content.trim().is_empty(), has_attachments) {
        (false, _) => content.to_owned(),
        (true, true) => "[attachment]".to_owned(),
        (true, false) => return None,
    };
    let text = if author["bot"].as_bool().unwrap_or(false) {
        let name = author["username"].as_str().unwrap_or("?");
        format!("[bot {name}] {text}")
    } else {
        text
    };
    Some((message["id"].as_str()?.to_owned(), text))
}

async fn update_thread(threads: &Arc<Mutex<Threads>>, thread: &Value) {
    let (Some(id), Some(parent_id)) = (thread["id"].as_str(), thread["parent_id"].as_str()) else {
        return;
    };
    threads
        .lock()
        .await
        .parents
        .insert(id.to_owned(), parent_id.to_owned());
}

async fn update_thread_event(
    threads: &Arc<Mutex<Threads>>,
    event: Option<&str>,
    data: &Value,
) -> bool {
    match event {
        Some("THREAD_CREATE") | Some("THREAD_UPDATE") => update_thread(threads, data).await,
        Some("THREAD_DELETE") => remove_thread(threads, data).await,
        Some("GUILD_CREATE") | Some("THREAD_LIST_SYNC") => {
            update_threads(threads, data["threads"].as_array()).await
        }
        _ => return false,
    }
    true
}

async fn update_threads(threads: &Arc<Mutex<Threads>>, values: Option<&Vec<Value>>) {
    for thread in values.into_iter().flatten() {
        update_thread(threads, thread).await;
    }
}

async fn remove_thread(threads: &Arc<Mutex<Threads>>, thread: &Value) {
    let Some(id) = thread["id"].as_str() else {
        return;
    };
    let mut threads = threads.lock().await;
    threads.parents.remove(id);
}

struct AnswerRequest {
    ctx: StartCtx,
    configured: Option<String>,
    data: std::path::PathBuf,
    root: std::path::PathBuf,
    token: String,
    channel: String,
    session_key: session::SessionKey,
    history_key: String,
    history_message_id: String,
    history: Arc<History>,
    history_limit: usize,
    clear_history_after_reply: bool,
    text: String,
    attachments: Vec<attachments::Attachment>,
    cancel: CancellationToken,
    sender: Option<AgentSender>,
    guards: Guards,
}

/// Loop-guard author for a turn: other bots are agents; humans are `None`.
fn turn_sender(message: &Value) -> Option<AgentSender> {
    message["author"]["bot"]
        .as_bool()
        .unwrap_or(false)
        .then(|| AgentSender {
            agent_id: format!(
                "discord:{}",
                message["author"]["id"].as_str().unwrap_or("?")
            ),
            hops: None,
        })
}

/// Ok(true) = silent turn (NO_REPLY or loop guard): nothing was posted.
async fn answer(request: AnswerRequest) -> Result<bool> {
    let AnswerRequest {
        ctx,
        configured,
        data,
        root,
        token,
        channel,
        session_key,
        history_key,
        history_message_id,
        history,
        history_limit,
        clear_history_after_reply,
        text,
        attachments,
        cancel,
        sender,
        guards,
    } = request;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let text = attachments::prompt(&client, &root, &attachments, text).await?;
    let text = history.prompt(&history_key, &history_message_id, &text, history_limit);
    let dir = session::prepare(&data, &session_key)?;
    let backend_id = dar_extension_sdk::chat::resolve_agent_backend(&ctx, configured.as_deref());
    let backend = ctx
        .host
        .services
        .get::<dyn ChatBackend>(&backend_id)
        .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
    let (tx, mut rx) = mpsc::channel(256);
    let params = dar_extension_sdk::chat::agent_session_params(&ctx, &dir)
        .resume_session_id(session::resume_id(&dir))
        .build();
    let admitted = guards
        .lock()
        .expect("loop guard lock poisoned")
        .entry(session_key.clone())
        .or_insert_with(|| cap_chat::LoopGuard::new(params.agent_loop))
        .admit(sender.as_ref());
    if let Err(reason) = admitted {
        dar_extension_sdk::log::event(
            "-",
            "discord",
            &format!("loop guard dropped turn ({})", reason.as_str()),
        );
        return Ok(true);
    }
    let mut chat = tokio::select! { _ = cancel.cancelled() => return Ok(false), result = backend.open(params, tx) => result? };
    tokio::select! { _ = cancel.cancelled() => { chat.abort().await?; chat.close().await?; return Ok(false) }, result = tokio::time::timeout(Duration::from_secs(60), chat.send_turn_from(text, sender)) => result.context("agent queue timed out")?? };
    let mut reply = String::new();
    let mut live = live_answer::LiveAnswer::new(
        reqwest::Client::new(),
        "https://discord.com/api/v10",
        &token,
        &channel,
    );
    let mut aborted = false;
    let mut silent = false;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => { chat.abort().await?; aborted = true; break },
            event = tokio::time::timeout(Duration::from_secs(60), rx.recv()) => match event.context("agent response timed out")? { Some(ChatEvent::Delta { role: ChatRole::Assistant, text }) => { reply.push_str(&text); live.push(&reply).await? }, Some(ChatEvent::Silent { .. }) => silent = true, Some(ChatEvent::TurnFinished { .. } | ChatEvent::SessionClosed { .. }) | None => break, Some(_) => {} },
            _ = live.wait_for_flush() => live.flush_if_due(&reply).await?
        }
    }
    chat.close().await?;
    if aborted {
        return Ok(false);
    }
    if silent {
        return Ok(true);
    }
    if reply.trim().is_empty() {
        reply = "(no response)".into()
    }
    live.finish(&reply).await?;
    if clear_history_after_reply {
        history.clear(&history_key);
    }
    Ok(false)
}

async fn next_json<S>(read: &mut S) -> Result<Value>
where
    S: futures_util::Stream<
            Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>,
        > + Unpin,
{
    loop {
        if let Some(value) = parse_message(read.next().await.context("Discord gateway closed")??)? {
            return Ok(value);
        }
    }
}
fn parse_message(message: Message) -> Result<Option<Value>> {
    match message {
        Message::Text(text) => Ok(Some(serde_json::from_str(&text)?)),
        Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => Ok(None),
        Message::Close(_) => anyhow::bail!("Discord gateway closed"),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_is_capped() {
        let mut delay = Duration::from_secs(1);
        for expected in [2, 4, 8, 16, 30, 30] {
            delay = reconnect_delay(delay);
            assert_eq!(delay, Duration::from_secs(expected));
        }
    }

    #[test]
    fn gateway_reconnect_opcodes_are_detected() {
        assert!(gateway_requests_reconnect(&json!({"op": 7})));
        assert!(gateway_requests_reconnect(&json!({"op": 9})));
        assert!(!gateway_requests_reconnect(&json!({"op": 0})));
    }

    #[tokio::test]
    async fn thread_create_routes_messages_through_the_parent_channel_config() {
        let threads = Arc::new(Mutex::new(Threads::default()));
        update_thread(&threads, &json!({"id":"t1", "parent_id":"c1"})).await;
        let parent_id = threads.lock().await.parents.get("t1").cloned();
        let cfg = config::DiscordConfig {
            guilds: HashMap::from([(
                "g1".into(),
                config::GuildConfig {
                    channels: HashMap::from([("c1".into(), config::ChannelConfig::default())]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert!(matches!(
            addressing::route(
                &cfg,
                Some("b1"),
                &addressing::InboundMessage {
                    guild_id: Some("g1"), channel_id: "t1", parent_channel_id: parent_id.as_deref(), thread_engaged: false,
                    author_id: "u1", author_is_bot: false, webhook_id: None, text: "<@b1> hello", has_attachments: false, mentions_bot: false,
                },
            ),
            addressing::RouteDecision::Dispatch { session_key, .. }
                if session_key == session::SessionKey::guild_thread("g1", "t1")
        ));
    }

    #[tokio::test]
    async fn guild_create_backfills_active_threads() {
        let threads = Arc::new(Mutex::new(Threads::default()));
        assert!(
            update_thread_event(
                &threads,
                Some("GUILD_CREATE"),
                &json!({"threads":[{"id":"t1", "parent_id":"c1"}]}),
            )
            .await
        );
        assert_eq!(
            threads.lock().await.parents.get("t1"),
            Some(&"c1".to_owned())
        );
    }

    #[tokio::test]
    async fn shutdown_interrupts_a_pending_reconnect_wait() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        tx.send(true).unwrap();
        let mut shutdown = dar_extension_sdk::ShutdownToken::new(rx);
        tokio::time::timeout(
            Duration::from_millis(50),
            wait_or_shutdown(&mut shutdown, Duration::from_secs(30)),
        )
        .await
        .expect("shutdown should not wait for reconnect backoff");
    }

    #[tokio::test]
    async fn shutdown_closes_a_live_gateway_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            matches!(socket.next().await.unwrap().unwrap(), Message::Close(_))
        });
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let (mut write, _) = socket.split();
        close_gateway(&mut write).await.unwrap();
        assert!(server.await.unwrap());
    }

    #[tokio::test]
    async fn shutdown_closes_a_gateway_waiting_for_hello() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            matches!(socket.next().await.unwrap().unwrap(), Message::Close(_))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let root =
            std::env::temp_dir().join(format!("discord-runtime-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let paths = host_api::HostPaths::new(&root).unwrap();
        let register = host_api::RegisterCtx {
            bus: host_api::EventBus::new(),
            http: host_api::HttpRegistry::disabled(),
            foreground: host_api::ForegroundRegistry::default(),
            services: host_api::ServiceRegistry::default(),
            paths: paths.clone(),
            config: host_api::ConfigStore::default(),
            shutdown: host_api::ShutdownToken::new(shutdown_rx.clone()),
        };
        let config = register.config.clone();
        let host = register.into_start_services().unwrap();
        let ctx = StartCtx {
            shutdown: host_api::ShutdownToken::new(shutdown_rx),
            paths,
            config,
            host,
        };
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            let turns = Arc::new(Mutex::new(HashMap::new()));
            run_connection(
                ConnectionEnv {
                    ctx: &ctx,
                    cfg: &config::DiscordConfig::default(),
                    token: "token",
                    data: ctx.paths.root(),
                    root: ctx.paths.root(),
                    client: &reqwest::Client::new(),
                    turns: &turns,
                    threads: &Arc::new(Mutex::new(Threads::default())),
                    history: &Arc::new(History::default()),
                    next_turn: &AtomicU64::new(0),
                    guards: &Guards::default(),
                },
                socket,
            )
            .await
        });
        shutdown_tx.send(true).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .is_ok());
        assert!(server.await.unwrap());
    }

    /// Backend whose sessions finish every turn silently (like NO_REPLY).
    struct SilentBackend {
        opens: Arc<std::sync::atomic::AtomicUsize>,
    }
    struct SilentSession(mpsc::Sender<ChatEvent>);
    impl ChatBackend for SilentBackend {
        fn open<'a>(
            &'a self,
            _: dar_extension_sdk::chat::ChatSessionParams,
            tx: mpsc::Sender<ChatEvent>,
        ) -> dar_extension_sdk::chat::BoxFuture<
            'a,
            Result<Box<dyn dar_extension_sdk::chat::ChatSession>>,
        > {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(Box::new(SilentSession(tx)) as Box<_>) })
        }
    }
    impl dar_extension_sdk::chat::ChatSession for SilentSession {
        fn send_turn(&mut self, _: String) -> dar_extension_sdk::chat::BoxFuture<'_, Result<()>> {
            let tx = self.0.clone();
            Box::pin(async move {
                tx.send(ChatEvent::Silent {
                    reason: None,
                    text: "NO_REPLY".into(),
                })
                .await?;
                tx.send(ChatEvent::TurnFinished {
                    ok: true,
                    error: None,
                })
                .await?;
                Ok(())
            })
        }
        fn abort(&mut self) -> dar_extension_sdk::chat::BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn close(self: Box<Self>) -> dar_extension_sdk::chat::BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn silent_ctx(
        opens: Arc<std::sync::atomic::AtomicUsize>,
    ) -> (StartCtx, tokio::sync::watch::Sender<bool>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let root = std::env::temp_dir().join(format!(
            "discord-silent-test-{}-{}",
            std::process::id(),
            opens.as_ref() as *const _ as usize
        ));
        std::fs::create_dir_all(&root).unwrap();
        let paths = host_api::HostPaths::new(&root).unwrap();
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
            .service::<dyn ChatBackend>("silent", Arc::new(SilentBackend { opens }))
            .unwrap();
        let config = register.config.clone();
        let host = register.into_start_services().unwrap();
        let ctx = StartCtx {
            shutdown: host_api::ShutdownToken::new(shutdown_rx),
            paths,
            config,
            host,
        };
        (ctx, shutdown_tx)
    }

    fn request(ctx: &StartCtx, guards: &Guards, sender: Option<AgentSender>) -> AnswerRequest {
        AnswerRequest {
            ctx: ctx.clone(),
            configured: Some("silent".into()),
            data: ctx.paths.root().to_path_buf(),
            root: ctx.paths.root().to_path_buf(),
            token: "token".into(),
            channel: "c1".into(),
            session_key: session::SessionKey::guild_channel("g1", "c1"),
            history_key: "h".into(),
            history_message_id: "m".into(),
            history: Arc::new(History::default()),
            history_limit: 20,
            clear_history_after_reply: false,
            text: "hi".into(),
            attachments: vec![],
            cancel: CancellationToken::new(),
            sender,
            guards: Arc::clone(guards),
        }
    }

    #[tokio::test]
    async fn silent_turn_posts_nothing() {
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ctx, _shutdown) = silent_ctx(Arc::clone(&opens));
        // No Discord server exists here: any post would fail the turn.
        assert!(answer(request(&ctx, &Guards::default(), None))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn loop_guard_spans_turns_and_humans_reset_it() {
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (ctx, _shutdown) = silent_ctx(Arc::clone(&opens));
        let guards = Guards::default();
        let bot = || {
            Some(AgentSender {
                agent_id: "discord:b2".into(),
                hops: None,
            })
        };
        let limit = dar_extension_sdk::chat::AgentLoopConfig::default().max_agent_turns as usize;
        for _ in 0..limit {
            answer(request(&ctx, &guards, bot())).await.unwrap();
        }
        assert_eq!(opens.load(Ordering::SeqCst), limit);
        assert!(answer(request(&ctx, &guards, bot())).await.unwrap());
        assert_eq!(
            opens.load(Ordering::SeqCst),
            limit,
            "blocked turn opens no session"
        );
        answer(request(&ctx, &guards, None)).await.unwrap();
        answer(request(&ctx, &guards, bot())).await.unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            limit + 2,
            "human turn reset the count"
        );
    }

    #[test]
    fn sender_only_for_bot_authors() {
        let bot = json!({"author": {"id": "b2", "bot": true}});
        assert_eq!(
            turn_sender(&bot),
            Some(AgentSender {
                agent_id: "discord:b2".into(),
                hops: None
            })
        );
        assert_eq!(turn_sender(&json!({"author": {"id": "u1"}})), None);
    }

    #[test]
    fn history_keeps_other_bots_but_not_self_or_webhooks() {
        let entry = |v: Value| history_entry(&v, Some("b1")).map(|(_, text)| text);
        assert_eq!(
            entry(
                json!({"id": "1", "content": "hi", "author": {"id": "b2", "bot": true, "username": "Pal"}})
            ),
            Some("[bot Pal] hi".into())
        );
        assert_eq!(
            entry(json!({"id": "2", "content": "hi", "author": {"id": "u1"}})),
            Some("hi".into())
        );
        assert_eq!(
            entry(json!({"id": "3", "content": "hi", "author": {"id": "b1", "bot": true}})),
            None
        );
        assert_eq!(
            entry(
                json!({"id": "4", "content": "hi", "webhook_id": "w", "author": {"id": "w", "bot": true}})
            ),
            None
        );
    }
}
