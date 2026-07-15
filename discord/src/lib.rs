//! Discord DM chat channel for dar.
use anyhow::{Context, Result};
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
    Extension, RegisterCtx, StartCtx,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
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
mod addressing;
mod attachments;
mod commands;
mod config;
mod delivery;
mod live_answer;
mod markdown;
mod runtime;
mod session;
pub fn extension() -> Box<dyn Extension> {
    Box::new(DiscordExtension)
}
struct DiscordExtension;

pub(crate) struct ActiveTurn {
    id: u64,
    cancel: CancellationToken,
    done: oneshot::Receiver<()>,
}

impl ActiveTurn {
    pub(crate) async fn stop(self) {
        self.cancel.cancel();
        let _ = self.done.await;
    }
}

impl Extension for DiscordExtension {
    fn id(&self) -> &'static str {
        "discord"
    }
    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            config::token(&config::parse(&ctx.config, self.id())?)?;
            Ok(())
        })
    }
    fn agent_singleton(&self) -> bool {
        true
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = config::parse(&ctx.config, self.id())?;
            let token = config::token(&cfg)?;
            let data = ctx.paths.data_dir(self.id())?;
            std::fs::create_dir_all(data.join("sessions"))?;
            let _ = run;
            tokio::spawn(async move {
                if let Err(error) = runtime::run(ctx, cfg, token, data).await {
                    tracing::error!(%error,"discord gateway stopped");
                }
            });
            Ok(())
        })
    }
}
pub(crate) async fn run(
    mut ctx: StartCtx,
    cfg: config::DiscordConfig,
    token: String,
    data: std::path::PathBuf,
) -> Result<()> {
    let client = reqwest::Client::new();
    let gateway: Gateway = client
        .get("https://discord.com/api/v10/gateway/bot")
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let (socket, _) =
        tokio_tungstenite::connect_async(format!("{}?v=10&encoding=json", gateway.url)).await?;
    let (mut write, mut read) = socket.split();
    let hello: Value = next_json(&mut read).await?;
    let interval = hello["d"]["heartbeat_interval"]
        .as_u64()
        .context("Discord gateway hello missing heartbeat_interval")?;
    write.send(Message::Text(json!({"op":2,"d":{"token":token,"intents":37376,"properties":{"os":"dar","browser":"dar","device":"dar"}}}).to_string())).await?;
    let mut heartbeat = tokio::time::interval(Duration::from_millis(interval));
    let mut sequence: Option<i64> = None;
    let mut bot_user_id: Option<String> = None;
    let turns: Arc<Mutex<HashMap<session::SessionKey, ActiveTurn>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let next_turn = AtomicU64::new(0);
    dar_extension_sdk::log::event("-", "discord", "gateway connected");
    loop {
        tokio::select! { _=ctx.shutdown.cancelled()=>return Ok(()), _=heartbeat.tick()=> { write.send(Message::Text(json!({"op":1,"d":sequence}).to_string())).await?; }, message=read.next()=> { let Some(message)=message else { anyhow::bail!("Discord gateway closed") }; let Some(value)=parse_message(message?)? else { continue }; if let Some(seq)=value["s"].as_i64(){sequence=Some(seq)}; if value["t"]=="READY" { bot_user_id=value["d"]["user"]["id"].as_str().map(str::to_owned); } if value["t"]=="MESSAGE_CREATE" { let d=&value["d"]; let route = addressing::route(&cfg, bot_user_id.as_deref(), &addressing::InboundMessage { guild_id: d["guild_id"].as_str(), channel_id: d["channel_id"].as_str().unwrap_or(""), author_id: d["author"]["id"].as_str().unwrap_or(""), author_is_bot: d["author"]["bot"].as_bool().unwrap_or(false), webhook_id: d["webhook_id"].as_str(), text: d["content"].as_str().unwrap_or("") }); let addressing::RouteDecision::Dispatch { text, session_key } = route else { continue }; if text.is_empty(){continue}; let channel=d["channel_id"].as_str().context("Discord message missing channel id")?.to_owned(); let message=d["id"].as_str().context("Discord message missing id")?.to_owned(); let delivery=delivery::Delivery::new(client.clone(), &token, &channel, &message, &cfg.ack_emoji); if let Err(error)=delivery.acknowledge().await { delivery.failure(&error).await; continue; } if let Some(command) = commands::parse(&text) { let previous = turns.lock().await.remove(&session_key); if let Some(turn) = previous { turn.stop().await; } if command == commands::Command::Reset { if let Err(error) = session::reset(&data, &session_key) { tracing::warn!(%error, "discord session reset failed"); delivery.failure(&error.into()).await; continue; } } if let Err(error) = delivery.post(commands::reply(command)).await { delivery.failure(&error).await; } continue; } let token=token.clone(); let backend=cfg.backend.clone(); let ctx=ctx.clone(); let data=data.clone(); let cancel=CancellationToken::new(); let task_cancel=cancel.clone(); let id=next_turn.fetch_add(1, Ordering::Relaxed); let (done_tx, done) = oneshot::channel(); let previous = turns.lock().await.insert(session_key.clone(), ActiveTurn { id, cancel: cancel.clone(), done }); if let Some(previous) = previous { previous.stop().await; } let active=Arc::clone(&turns); tokio::spawn(async move { if let Err(error)=answer(ctx,backend,&data,&token,&channel,session_key.clone(),text,cancel).await { if !task_cancel.is_cancelled() { tracing::warn!(%error,"discord turn failed"); delivery.failure(&error).await; } } let _ = done_tx.send(()); let mut active=active.lock().await; if active.get(&session_key).is_some_and(|turn| turn.id == id) { active.remove(&session_key); } }); } } }
    }
}
pub(crate) async fn answer(
    ctx: StartCtx,
    configured: Option<String>,
    data: &Path,
    token: &str,
    channel: &str,
    session_key: session::SessionKey,
    text: String,
    cancel: CancellationToken,
) -> Result<()> {
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
    let mut chat = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = backend.open(params, tx) => result?,
    };
    tokio::select! {
        _ = cancel.cancelled() => {
            chat.abort().await?;
            chat.close().await?;
            return Ok(());
        }
        result = tokio::time::timeout(Duration::from_secs(60), chat.send_turn(text)) => {
            result.context("agent queue timed out")??;
        }
    }
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
            _ = cancel.cancelled() => {
                chat.abort().await?;
                aborted = true;
                break;
            }
            event = tokio::time::timeout(Duration::from_secs(60), rx.recv()) => match event.context("agent response timed out")? {
                Some(ChatEvent::Delta { role: ChatRole::Assistant, text }) => {
                    reply.push_str(&text);
                    live.push(&reply).await?;
                }
                Some(ChatEvent::TurnFinished { .. } | ChatEvent::SessionClosed { .. }) | None => break,
                Some(_) => {}
            },
            _ = live.wait_for_flush() => {
                live.flush_if_due(&reply).await?;
            }
        }
    }
    chat.close().await?;
    if aborted {
        return Ok(());
    }
    if reply.trim().is_empty() {
        reply = "(no response)".into()
    };
    live.finish(&reply).await?;
    Ok(())
}

#[derive(Deserialize)]
struct Gateway {
    url: String,
}
pub(crate) async fn next_json<S>(read: &mut S) -> Result<Value>
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
pub(crate) fn parse_message(message: Message) -> Result<Option<Value>> {
    match message {
        Message::Text(text) => Ok(Some(serde_json::from_str(&text)?)),
        Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => Ok(None),
        Message::Close(_) => anyhow::bail!("Discord gateway closed"),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {}
