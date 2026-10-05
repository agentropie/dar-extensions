//! Inbound-only WhatsApp linked-device channel.
use anyhow::{bail, Context, Result};
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
    ConfigStore, Extension, RegisterCtx, StartCtx,
};
use serde::Deserialize;
use std::{collections::HashMap, path::Path, sync::Mutex, time::Duration};
use tokio::sync::mpsc;
use whatsapp_rust::{
    pair_code::PairCodeOptions,
    prelude::{Bot, MessageContext, MessageExt, SqliteStore},
};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct Config {
    phone_number: Option<String>,
    allowed_users: Vec<String>,
    backend: Option<String>,
}
fn digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit())
}
fn parse(config: &ConfigStore) -> Result<Config> {
    let cfg: Config = config
        .get("whatsapp-web")
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();
    for phone in cfg.phone_number.iter().chain(cfg.allowed_users.iter()) {
        if !digits(phone) {
            bail!("whatsapp-web phone_number and allowed_users must contain ASCII digits only");
        }
    }
    Ok(cfg)
}
pub struct WhatsAppWebExtension {
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
pub fn extension() -> Box<dyn Extension> {
    Box::new(WhatsAppWebExtension {
        task: Mutex::new(None),
    })
}
impl Extension for WhatsAppWebExtension {
    fn id(&self) -> &'static str {
        "whatsapp-web"
    }
    fn agent_singleton(&self) -> bool {
        true
    }
    fn register<'a>(
        &'a self,
        ctx: &'a mut RegisterCtx,
    ) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move { parse(&ctx.config).map(|_| ()) })
    }
    fn start<'a>(&'a self, ctx: StartCtx) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cfg = parse(&ctx.config)?;
            let root = ctx.paths.root().join(".whatsapp-web");
            std::fs::create_dir_all(&root)?;
            std::fs::write(root.join(".gitignore"), "*\n")?;
            let db = root.join("session.db");
            let store =
                SqliteStore::open(db.to_str().context("session store path is not UTF-8")?).await?;
            let (tx, rx) = mpsc::channel(256);
            let mut builder = Bot::builder().with_backend(store)
                .on_message(move |message| { let tx = tx.clone(); async move { let _ = tx.send(message).await; } })
                .on_pair_code(|code, timeout| async move {
                    tracing::info!(%code, seconds = timeout.as_secs(), "whatsapp-web pairing code: enter in Linked devices");
                })
                .on_pair_code_error(|error, _| async move { tracing::warn!(?error, "whatsapp-web pairing code request failed"); })
                .on_qr_code(|code, _| async move { tracing::info!(%code, "whatsapp-web pairing QR fallback"); });
            if let Some(phone_number) = cfg.phone_number.clone() {
                builder = builder.with_pair_code(PairCodeOptions {
                    phone_number,
                    ..Default::default()
                });
            }
            let bot = builder.build().await?;
            tracing::info!(path = %db.display(), "whatsapp-web session store opened");
            let task = tokio::spawn(async move {
                dispatch(ctx, cfg, root.join("sessions"), rx, bot).await;
            });
            *self.task.lock().unwrap() = Some(task);
            Ok(())
        })
    }
    fn stop<'a>(&'a self) -> dar_extension_sdk::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let Some(mut task) = self.task.lock().unwrap().take() else {
                return Ok(());
            };
            match tokio::time::timeout(Duration::from_secs(4), &mut task).await {
                Ok(joined) => joined.context("whatsapp-web worker failed"),
                Err(_) => {
                    task.abort();
                    bail!("whatsapp-web shutdown timed out")
                }
            }
        })
    }
}
#[derive(Clone)]
struct Inbound {
    session_key: String,
    phone: Option<String>,
    text: String,
}
async fn from_message(message: &MessageContext) -> Option<Inbound> {
    let source = &message.info.source;
    if source.is_from_me
        || source.is_group
        || !(source.chat.is_pn() || source.chat.is_lid())
        || message.info.edit != whatsapp_rust::types::message::EditAttribute::Empty
        || whatsapp_rust::types::message::EditAttribute::infer_from_message(&message.message)
            .is_some()
    {
        return None;
    }
    let text = message.message.text_content()?.trim().to_owned();
    if text.is_empty() {
        return None;
    }
    let phone = if source.sender.is_pn() {
        Some(source.sender.user_base().to_owned())
    } else if let Some(alt) = source.sender_alt.as_ref().filter(|jid| jid.is_pn()) {
        Some(alt.user_base().to_owned())
    } else if source.sender.is_lid() {
        message
            .client
            .get_lid_pn_entry(&source.sender)
            .await
            .ok()
            .flatten()
            .map(|entry| entry.phone_number.to_string())
    } else {
        None
    }
    .filter(|phone| digits(phone));
    let session_key = if let Some(phone) = &phone {
        format!("pn-{phone}")
    } else if source.sender.is_lid() && digits(source.sender.user_base()) {
        format!("lid-{}", source.sender.user_base())
    } else {
        return None;
    };
    Some(Inbound {
        session_key,
        phone,
        text,
    })
}
trait Transport: Clone + Send + Sync + 'static {
    async fn read(&self) -> Result<()>;
    async fn typing(&self, active: bool) -> Result<()>;
    async fn send(&self, text: String) -> Result<()>;
}
impl Transport for MessageContext {
    async fn read(&self) -> Result<()> {
        self.client.mark_message_read(&self.message_ref()?).await?;
        Ok(())
    }
    async fn typing(&self, active: bool) -> Result<()> {
        let chat = &self.info.source.chat;
        if active {
            self.client.chatstate().send_composing(chat).await?;
        } else {
            self.client.chatstate().send_paused(chat).await?;
        }
        Ok(())
    }
    async fn send(&self, text: String) -> Result<()> {
        self.reply_quoting(text).await?;
        Ok(())
    }
}
struct Connection {
    session: Box<dyn dar_extension_sdk::chat::ChatSession>,
    events: mpsc::Receiver<ChatEvent>,
}
async fn dispatch(
    ctx: StartCtx,
    cfg: Config,
    root: std::path::PathBuf,
    mut rx: mpsc::Receiver<MessageContext>,
    bot: Bot,
) {
    let mut handle = bot.spawn();
    let mut sessions = HashMap::new();
    let mut shutdown = ctx.shutdown.clone();
    let mut completed = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            outcome = &mut handle => { tracing::warn!(?outcome, "whatsapp-web connection stopped"); completed = true; break; },
            message = rx.recv() => {
                let Some(message) = message else { break; };
                let result = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = async {
                        if let Some(inbound) = from_message(&message).await {
                            process(&ctx, &cfg, &root, inbound, message, &mut sessions).await
                        } else { Ok(()) }
                    } => result,
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "whatsapp-web inbound turn failed");
                }
            }
        }
    }
    if tokio::time::timeout(Duration::from_secs(3), handle.client().shutdown())
        .await
        .is_err()
    {
        tracing::warn!("whatsapp-web client shutdown timed out");
        handle.abort();
    } else if !completed
        && tokio::time::timeout(Duration::from_millis(500), &mut handle)
            .await
            .is_err()
    {
        handle.abort();
    }
    for (_, connection) in sessions {
        let _ = tokio::time::timeout(Duration::from_millis(100), connection.session.close()).await;
    }
}
async fn process<T: Transport>(
    ctx: &StartCtx,
    cfg: &Config,
    root: &Path,
    inbound: Inbound,
    transport: T,
    sessions: &mut HashMap<String, Connection>,
) -> Result<()> {
    if !cfg.allowed_users.is_empty()
        && !inbound
            .phone
            .as_ref()
            .is_some_and(|p| cfg.allowed_users.contains(p))
    {
        return Ok(());
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), transport.read()).await;
    if !sessions.contains_key(&inbound.session_key) {
        let backend_id =
            dar_extension_sdk::chat::resolve_agent_backend(ctx, cfg.backend.as_deref());
        let backend = ctx
            .host
            .services
            .get::<dyn ChatBackend>(&backend_id)
            .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
        let session_dir = root.join(&inbound.session_key);
        std::fs::create_dir_all(&session_dir)?;
        let (tx, events) = mpsc::channel(256);
        let session = tokio::time::timeout(
            Duration::from_secs(30),
            backend.open(
                dar_extension_sdk::chat::agent_session_params(ctx, &session_dir).build(),
                tx,
            ),
        )
        .await
        .context("chat backend open timed out")??;
        sessions.insert(inbound.session_key.clone(), Connection { session, events });
    }
    let connection = sessions
        .get_mut(&inbound.session_key)
        .context("session missing")?;
    let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(true)).await;
    let result = {
        let turn = tokio::time::timeout(Duration::from_secs(300), turn(connection, inbound.text));
        tokio::pin!(turn);
        loop {
            tokio::select! {
                result = &mut turn => break result.context("chat turn timed out").and_then(|result| result),
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(true)).await;
                }
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(false)).await;
    if result.is_err() {
        if let Some(connection) = sessions.remove(&inbound.session_key) {
            let _ = tokio::time::timeout(Duration::from_secs(2), connection.session.close()).await;
        }
    }
    let reply = result.unwrap_or_else(|_| "(turn failed)".into());
    if !ctx.shutdown.is_cancelled() && !reply.trim().is_empty() {
        tokio::time::timeout(
            Duration::from_secs(20),
            transport.send(adapt_markdown(&reply)),
        )
        .await
        .context("whatsapp send timed out")??;
    }
    Ok(())
}
async fn turn(connection: &mut Connection, text: String) -> Result<String> {
    connection.session.send_turn(text).await?;
    let mut reply = String::new();
    let mut silent = false;
    while let Some(event) = connection.events.recv().await {
        match event {
            ChatEvent::Delta {
                role: ChatRole::Assistant,
                text,
            } if !silent => reply.push_str(&text),
            ChatEvent::Silent { .. } => {
                silent = true;
                reply.clear();
            }
            ChatEvent::TurnFinished { ok: true, .. } => return Ok(reply),
            ChatEvent::TurnFinished { error, .. } | ChatEvent::SessionClosed { error } => {
                bail!("{}", error.unwrap_or_else(|| "backend failed".into()))
            }
            _ => {}
        }
    }
    bail!("backend event stream closed")
}
fn adapt_markdown(text: &str) -> String {
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
    use std::sync::Arc;
    use tokio::sync::watch;
    struct EchoBackend;
    struct EchoSession(mpsc::Sender<ChatEvent>);
    impl ChatBackend for EchoBackend {
        fn open<'a>(
            &'a self,
            _: ChatSessionParams,
            events: mpsc::Sender<ChatEvent>,
        ) -> BoxFuture<'a, Result<Box<dyn ChatSession>>> {
            Box::pin(async move { Ok(Box::new(EchoSession(events)) as Box<dyn ChatSession>) })
        }
    }
    impl ChatSession for EchoSession {
        fn send_turn(&mut self, prompt: String) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                if prompt == "hang" {
                    return std::future::pending().await;
                }
                if prompt == "fail" {
                    self.0
                        .send(ChatEvent::TurnFinished {
                            ok: false,
                            error: Some("failed".into()),
                        })
                        .await?;
                    return Ok(());
                }
                self.0
                    .send(ChatEvent::Delta {
                        role: ChatRole::Assistant,
                        text: prompt,
                    })
                    .await?;
                self.0
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
    #[derive(Clone, Default)]
    struct FakeTransport(Arc<Mutex<Vec<String>>>);
    impl Transport for FakeTransport {
        async fn read(&self) -> Result<()> {
            self.0.lock().unwrap().push("read".into());
            Ok(())
        }
        async fn typing(&self, active: bool) -> Result<()> {
            self.0.lock().unwrap().push(format!("typing:{active}"));
            Ok(())
        }
        async fn send(&self, text: String) -> Result<()> {
            self.0.lock().unwrap().push(format!("send:{text}"));
            Ok(())
        }
    }
    #[derive(Clone, Default)]
    struct HungReadTransport(FakeTransport);
    impl Transport for HungReadTransport {
        async fn read(&self) -> Result<()> {
            std::future::pending().await
        }
        async fn typing(&self, active: bool) -> Result<()> {
            self.0.typing(active).await
        }
        async fn send(&self, text: String) -> Result<()> {
            self.0.send(text).await
        }
    }
    #[derive(Clone, Default)]
    struct HungSendTransport(FakeTransport);
    impl Transport for HungSendTransport {
        async fn read(&self) -> Result<()> {
            self.0.read().await
        }
        async fn typing(&self, active: bool) -> Result<()> {
            self.0.typing(active).await
        }
        async fn send(&self, _: String) -> Result<()> {
            std::future::pending().await
        }
    }
    fn context(root: &Path) -> (StartCtx, watch::Sender<bool>) {
        let (tx, rx) = watch::channel(false);
        let paths = host_api::HostPaths::new(root).unwrap();
        let mut register = RegisterCtx {
            bus: host_api::EventBus::new(),
            http: host_api::HttpRegistry::disabled(),
            foreground: host_api::ForegroundRegistry::default(),
            services: host_api::ServiceRegistry::default(),
            paths: paths.clone(),
            config: ConfigStore::default(),
            shutdown: host_api::ShutdownToken::new(rx.clone()),
        };
        register
            .services
            .service::<dyn ChatBackend>("pi", Arc::new(EchoBackend))
            .unwrap();
        let config = register.config.clone();
        let host = register.into_start_services().unwrap();
        (
            StartCtx {
                shutdown: host_api::ShutdownToken::new(rx),
                paths,
                config,
                host,
            },
            tx,
        )
    }
    #[tokio::test]
    async fn inbound_turn_sends_reply_and_allowlist_filters() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = Inbound {
            session_key: "pn-3361".into(),
            phone: Some("3361".into()),
            text: "hello".into(),
        };
        let mut sessions = HashMap::new();
        let cfg = Config {
            allowed_users: vec!["999".into()],
            ..Default::default()
        };
        process(
            &ctx,
            &cfg,
            temp.path(),
            inbound.clone(),
            transport.clone(),
            &mut sessions,
        )
        .await
        .unwrap();
        assert!(transport.0.lock().unwrap().is_empty());
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
            &mut sessions,
        )
        .await
        .unwrap();
        let actions = transport.0.lock().unwrap();
        assert!(actions.contains(&"read".into()));
        assert!(actions.contains(&"typing:true".into()));
        assert!(actions.contains(&"typing:false".into()));
        assert!(actions.contains(&"send:hello".into()));
    }
    #[tokio::test]
    async fn unresolved_lid_is_rejected_by_allowlist() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = Inbound {
            session_key: "lid-123".into(),
            phone: None,
            text: "hello".into(),
        };
        let cfg = Config {
            allowed_users: vec!["123".into()],
            ..Default::default()
        };
        process(
            &ctx,
            &cfg,
            temp.path(),
            inbound,
            transport.clone(),
            &mut HashMap::new(),
        )
        .await
        .unwrap();
        assert!(transport.0.lock().unwrap().is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn hung_read_does_not_block_turn() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = HungReadTransport::default();
        let inbound = Inbound {
            session_key: "pn-3361".into(),
            phone: Some("3361".into()),
            text: "hello".into(),
        };
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
            &mut HashMap::new(),
        )
        .await
        .unwrap();
        assert!(transport
            .0
             .0
            .lock()
            .unwrap()
            .contains(&"send:hello".into()));
    }
    #[tokio::test]
    async fn failed_turn_cleans_session_and_replies() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = Inbound {
            session_key: "pn-3361".into(),
            phone: Some("3361".into()),
            text: "fail".into(),
        };
        let mut sessions = HashMap::new();
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
            &mut sessions,
        )
        .await
        .unwrap();
        assert!(sessions.is_empty());
        assert!(transport
            .0
            .lock()
            .unwrap()
            .contains(&"send:(turn failed)".into()));
    }
    #[tokio::test(start_paused = true)]
    async fn hung_turn_times_out_and_cleans_session() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut sessions = HashMap::new();
        let started = tokio::time::Instant::now();
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            Inbound {
                session_key: "pn-3361".into(),
                phone: Some("3361".into()),
                text: "hang".into(),
            },
            transport.clone(),
            &mut sessions,
        )
        .await
        .unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(300));
        assert!(sessions.is_empty());
        let actions = transport.0.lock().unwrap();
        assert!(actions.contains(&"typing:false".into()));
        assert!(actions.contains(&"send:(turn failed)".into()));
    }
    #[tokio::test(start_paused = true)]
    async fn hung_send_times_out() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = HungSendTransport::default();
        let started = tokio::time::Instant::now();
        let error = process(
            &ctx,
            &Config::default(),
            temp.path(),
            Inbound {
                session_key: "pn-3361".into(),
                phone: Some("3361".into()),
                text: "hello".into(),
            },
            transport.clone(),
            &mut HashMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "whatsapp send timed out");
        assert_eq!(started.elapsed(), Duration::from_secs(20));
        assert!(transport
            .0
             .0
            .lock()
            .unwrap()
            .contains(&"typing:false".into()));
    }
    #[test]
    fn phone_validation() {
        assert!(digits("3361"));
        assert!(!digits("+3361"));
    }
}
