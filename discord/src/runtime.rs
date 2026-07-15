use anyhow::{Context, Result};
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
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

use crate::{addressing, attachments, commands, config, delivery, live_answer, session, Gateway};

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
    next_turn: &'a AtomicU64,
}

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
    let next_turn = AtomicU64::new(0);
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
            result = tokio_tungstenite::connect_async(format!("{gateway}?v=10&encoding=json")) => result,
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
                next_turn: &next_turn,
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
                if value["t"] == "READY" { bot_user_id = value["d"]["user"]["id"].as_str().map(str::to_owned); }
                match value["t"].as_str() {
                    Some("THREAD_CREATE") | Some("THREAD_UPDATE") => update_thread(env.threads, &value["d"]).await,
                    Some("THREAD_DELETE") => remove_thread(env.threads, &value["d"]).await,
                    Some("THREAD_LIST_SYNC") => update_threads(env.threads, value["d"]["threads"].as_array()).await,
                    Some("MESSAGE_CREATE") => {
                        handle_message(env.ctx, env.cfg, env.token, env.data, env.root, env.client, env.turns, env.threads, env.next_turn, bot_user_id.as_deref(), &value["d"]).await;
                    }
                    _ => {}
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

async fn handle_message(
    ctx: &StartCtx,
    cfg: &config::DiscordConfig,
    token: &str,
    data: &Path,
    root: &Path,
    client: &reqwest::Client,
    turns: &Arc<Mutex<HashMap<session::SessionKey, ActiveTurn>>>,
    threads: &Arc<Mutex<Threads>>,
    next_turn: &AtomicU64,
    bot_user_id: Option<&str>,
    message: &Value,
) {
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
    let thread_engaged = thread_session_key
        .as_ref()
        .is_some_and(|key| session::is_engaged(data, key));
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
        },
    );
    let addressing::RouteDecision::Dispatch { session_key, .. } = route else {
        return;
    };
    if parent_channel_id.is_some() {
        if let Err(error) = session::engage(data, &session_key) {
            tracing::warn!(%error, "discord thread engagement could not be persisted");
            return;
        }
    }
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
    if let Err(error) = delivery.acknowledge().await {
        delivery.failure(&error).await;
        return;
    }
    if let Some(command) = commands::parse(content) {
        if let Some(turn) = turns.lock().await.remove(&session_key) {
            turn.stop().await;
        }
        if command == commands::Command::Reset {
            if let Err(error) = session::reset(data, &session_key) {
                delivery.failure(&error.into()).await;
                return;
            }
        }
        if let Err(error) = delivery.post(commands::reply(command)).await {
            delivery.failure(&error).await;
        }
        return;
    }
    let token = token.to_owned();
    let backend = cfg.backend.clone();
    let ctx = ctx.clone();
    let data = data.to_path_buf();
    let root = root.to_path_buf();
    let channel = channel.to_owned();
    let prompt = content.to_owned();
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
        if let Err(error) = answer(
            ctx,
            backend,
            &data,
            &root,
            &token,
            &channel,
            session_key.clone(),
            prompt,
            attachments,
            cancel,
        )
        .await
        {
            if !task_cancel.is_cancelled() {
                tracing::warn!(%error, "discord turn failed");
                delivery.failure(&error).await;
            }
        }
        let _ = done_tx.send(());
        let mut turns = turns.lock().await;
        if turns.get(&session_key).is_some_and(|turn| turn.id == id) {
            turns.remove(&session_key);
        }
    });
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

async fn answer(
    ctx: StartCtx,
    configured: Option<String>,
    data: &Path,
    root: &Path,
    token: &str,
    channel: &str,
    session_key: session::SessionKey,
    text: String,
    attachments: Vec<attachments::Attachment>,
    cancel: CancellationToken,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let text = attachments::prompt(&client, root, &attachments, text).await?;
    let dir = session::prepare(data, &session_key)?;
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
    let mut chat = tokio::select! { _ = cancel.cancelled() => return Ok(()), result = backend.open(params, tx) => result? };
    tokio::select! { _ = cancel.cancelled() => { chat.abort().await?; chat.close().await?; return Ok(()) }, result = tokio::time::timeout(Duration::from_secs(60), chat.send_turn(text)) => result.context("agent queue timed out")?? };
    let mut reply = String::new();
    let mut live = live_answer::LiveAnswer::new(
        reqwest::Client::new(),
        "https://discord.com/api/v10",
        token,
        channel,
    );
    let mut aborted = false;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => { chat.abort().await?; aborted = true; break },
            event = tokio::time::timeout(Duration::from_secs(60), rx.recv()) => match event.context("agent response timed out")? { Some(ChatEvent::Delta { role: ChatRole::Assistant, text }) => { reply.push_str(&text); live.push(&reply).await? }, Some(ChatEvent::TurnFinished { .. } | ChatEvent::SessionClosed { .. }) | None => break, Some(_) => {} },
            _ = live.wait_for_flush() => live.flush_if_due(&reply).await?
        }
    }
    chat.close().await?;
    if aborted {
        return Ok(());
    }
    if reply.trim().is_empty() {
        reply = "(no response)".into()
    }
    live.finish(&reply).await?;
    Ok(())
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
                    author_id: "u1", author_is_bot: false, webhook_id: None, text: "<@b1> hello", has_attachments: false,
                },
            ),
            addressing::RouteDecision::Dispatch { session_key, .. }
                if session_key == session::SessionKey::guild_thread("g1", "t1")
        ));
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
                    next_turn: &AtomicU64::new(0),
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
}
