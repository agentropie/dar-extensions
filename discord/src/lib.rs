//! Discord DM chat channel for dar.
use anyhow::{Context, Result};
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
    Extension, RegisterCtx, StartCtx,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
mod addressing;
mod config;
mod live_answer;
mod markdown;
mod session;
pub fn extension() -> Box<dyn Extension> {
    Box::new(DiscordExtension)
}
struct DiscordExtension;
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
            tokio::spawn(async move {
                if let Err(error) = run(ctx, cfg, token, data).await {
                    tracing::error!(%error,"discord gateway stopped");
                }
            });
            Ok(())
        })
    }
}
async fn run(
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
    dar_extension_sdk::log::event("-", "discord", "gateway connected");
    loop {
        tokio::select! { _=ctx.shutdown.cancelled()=>return Ok(()), _=heartbeat.tick()=> { write.send(Message::Text(json!({"op":1,"d":sequence}).to_string())).await?; }, message=read.next()=> { let Some(message)=message else { anyhow::bail!("Discord gateway closed") }; let Some(value)=parse_message(message?)? else { continue }; if let Some(seq)=value["s"].as_i64(){sequence=Some(seq)}; if value["t"]=="READY" { bot_user_id=value["d"]["user"]["id"].as_str().map(str::to_owned); } if value["t"]=="MESSAGE_CREATE" { let d=&value["d"]; let route = addressing::route(&cfg, bot_user_id.as_deref(), &addressing::InboundMessage { guild_id: d["guild_id"].as_str(), channel_id: d["channel_id"].as_str().unwrap_or(""), author_id: d["author"]["id"].as_str().unwrap_or(""), author_is_bot: d["author"]["bot"].as_bool().unwrap_or(false), webhook_id: d["webhook_id"].as_str(), text: d["content"].as_str().unwrap_or("") }); let addressing::RouteDecision::Dispatch { text, session_key } = route else { continue }; if text.is_empty(){continue}; let channel=d["channel_id"].as_str().context("Discord message missing channel id")?.to_owned(); let token=token.clone(); let backend=cfg.backend.clone(); let ctx=ctx.clone(); let data=data.clone(); tokio::spawn(async move { if let Err(error)=answer(ctx,backend,&data,&token,&channel,session_key,text).await { tracing::warn!(%error,"discord turn failed") }}); } } }
    }
}
async fn answer(
    ctx: StartCtx,
    configured: Option<String>,
    data: &Path,
    token: &str,
    channel: &str,
    session_key: session::SessionKey,
    text: String,
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
    let mut chat = backend.open(params, tx).await?;
    chat.send_turn(text).await?;
    let mut reply = String::new();
    let mut live = live_answer::LiveAnswer::new(
        reqwest::Client::new(),
        "https://discord.com/api/v10",
        token,
        channel,
    );
    loop {
        tokio::select! {
            event = rx.recv() => match event {
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
mod tests {}
