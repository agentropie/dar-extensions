//! Inbound-only WhatsApp linked-device channel.
use anyhow::{bail, Context, Result};
use dar_extension_sdk::{
    chat::{ChatBackend, ChatEvent, ChatRole},
    ConfigStore, Extension, RegisterCtx, StartCtx,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use whatsapp_rust::{
    download::Downloadable,
    pair_code::PairCodeOptions,
    prelude::{wa, Bot, Event, EventKind, MessageContext, MessageExt, SqliteStore},
};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct Config {
    phone_number: Option<String>,
    allowed_users: Vec<String>,
    allowed_groups: Vec<String>,
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
    if cfg
        .allowed_groups
        .iter()
        .any(|group| group.is_empty() || !group.bytes().all(|c| c.is_ascii_digit() || c == b'-'))
    {
        bail!("whatsapp-web allowed_groups must contain group id digits only");
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
            // The library's own already-paired check runs before a stored session
            // finishes logging in, so it would request a code on every restart.
            let paired = store
                .database()
                .list_devices()
                .await?
                .iter()
                .any(|device| device.linked);
            let (tx, rx) = mpsc::channel(256);
            let qr_shown = Arc::new(AtomicBool::new(false));
            let mut builder = Bot::builder()
                .with_backend(store)
                .on_message(move |message| {
                    let tx = tx.clone();
                    async move {
                        let _ = tx.send(message).await;
                    }
                })
                .on_pair_code(|code, timeout| async move {
                    dar_extension_sdk::log::event(
                        "-",
                        "whatsapp-web",
                        &format!(
                            "pairing code {code} (valid {}s): enter in Linked devices",
                            timeout.as_secs()
                        ),
                    );
                })
                .on_pair_code_error(|error, _| async move {
                    dar_extension_sdk::log::event(
                        "-",
                        "whatsapp-web",
                        &format!("pairing code request failed: {error:?}"),
                    );
                })
                .on_event_for(&[EventKind::Connected], |_, client| async move {
                    dar_extension_sdk::log::event(
                        "-",
                        "whatsapp-web",
                        &connected_message(client.pn().as_ref().map(|jid| jid.user_base())),
                    );
                })
                .on_event_for(
                    &[
                        EventKind::PairSuccess,
                        EventKind::PairError,
                        EventKind::LoggedOut,
                    ],
                    |event, _| async move {
                        if let Some(message) = pairing_message(&event) {
                            dar_extension_sdk::log::event("-", "whatsapp-web", &message);
                        }
                    },
                )
                .on_qr_code(move |code, _| {
                    let first = !qr_shown.swap(true, Ordering::Relaxed);
                    async move {
                        tracing::info!(%code, "whatsapp-web pairing QR fallback");
                        if first {
                            log_qr(&code);
                        }
                    }
                });
            if let Some(phone_number) = cfg.phone_number.clone().filter(|_| !paired) {
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
const MAX_PENDING: usize = 20;
const MAX_RECENT: usize = 200;
const MAX_SNIPPET: usize = 200;
const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;
const MAX_ATTACHMENTS: usize = 10;
#[derive(Clone, Debug, PartialEq)]
enum Kind {
    Turn,
    Unaddressed,
    Reaction { emoji: String, target: String },
}
#[derive(Clone, Debug, Default, PartialEq)]
struct Reply {
    who: Option<String>,
    text: Option<String>,
}
#[derive(Clone, Debug, Default, PartialEq)]
struct Header {
    group: Option<String>,
    name: String,
    phone: Option<String>,
    lid: Option<String>,
    time: String,
    reply: Option<Reply>,
    forwarded: bool,
}
#[derive(Clone)]
struct Inbound {
    session_key: String,
    phone: Option<String>,
    group: Option<String>,
    message_id: String,
    kind: Kind,
    header: Header,
    text: String,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
struct Attachment {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped: Option<String>,
}
fn allowed(cfg: &Config, phone: Option<&str>, group: Option<&str>) -> bool {
    (cfg.allowed_users.is_empty()
        || phone.is_some_and(|p| cfg.allowed_users.iter().any(|u| u == p)))
        && group.is_none_or(|g| {
            cfg.allowed_groups.is_empty() || cfg.allowed_groups.contains(&g.to_owned())
        })
}
/// User part of a JID string, without server or device suffix.
fn jid_user(jid: &str) -> &str {
    let user = jid.split('@').next().unwrap_or(jid);
    user.split(':').next().unwrap_or(user)
}
fn mentions_bot(mentioned: &[String], bot_users: &[String]) -> bool {
    mentioned
        .iter()
        .any(|jid| bot_users.iter().any(|bot| bot == jid_user(jid)))
}
/// Decides what an inbound message with content does: DMs and group mentions start
/// turns; other group messages are only kept as context.
fn route(is_group: bool, mentioned: bool, has_content: bool) -> Option<Kind> {
    match (has_content, !is_group || mentioned) {
        (false, _) => None,
        (true, true) => Some(Kind::Turn),
        (true, false) => Some(Kind::Unaddressed),
    }
}
/// Single-line text of at most `max` characters.
fn snippet(text: &str, max: usize) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if line.chars().count() <= max {
        return line;
    }
    let mut cut: String = line.chars().take(max).collect();
    cut.push('…');
    cut
}
fn format_time<Tz: chrono::TimeZone>(time: &chrono::DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    time.format("%Y-%m-%d %H:%M %:z").to_string()
}
fn identity_label(name: &str, phone: Option<&str>, lid: Option<&str>) -> String {
    let id = match (phone, lid) {
        (Some(phone), _) => Some(format!("+{phone}")),
        (None, Some(lid)) => Some(format!("lid {lid}")),
        _ => None,
    };
    match (name.trim(), id) {
        ("", None) => "someone".into(),
        ("", Some(id)) => id,
        (name, _) => name.into(),
    }
}
fn sender_label(header: &Header) -> String {
    identity_label(&header.name, header.phone.as_deref(), header.lid.as_deref())
}
fn describe_participant(jid: &str, bot_users: &[String]) -> String {
    let user = jid_user(jid);
    if bot_users.iter().any(|bot| bot == user) {
        "you".into()
    } else if jid.contains("@lid") {
        format!("lid {user}")
    } else {
        format!("+{user}")
    }
}
fn header_text(header: &Header, subject: Option<&str>) -> String {
    let place = match (&header.group, subject) {
        (None, _) => "DM".to_owned(),
        (Some(id), Some(subject)) => format!("group \"{subject}\" ({id})"),
        (Some(id), None) => format!("group ({id})"),
    };
    let mut from = Vec::new();
    if !header.name.trim().is_empty() {
        from.push(format!("from {}", header.name.trim()));
    }
    match (&header.phone, &header.lid) {
        (Some(phone), _) => from.push(format!("+{phone}")),
        (None, Some(lid)) => from.push(format!("lid {lid}")),
        _ => {}
    }
    let mut parts = vec![format!("WhatsApp {place}")];
    parts.extend(from);
    parts.push(header.time.clone());
    let mut text = format!("[{}]", parts.join(" · "));
    if let Some(reply) = &header.reply {
        text.push_str("\n↪ replying to");
        if let Some(who) = &reply.who {
            text.push(' ');
            text.push_str(who);
        }
        if let Some(quoted) = reply.text.as_ref().filter(|t| !t.is_empty()) {
            text.push_str(&format!(": \"{}\"", snippet(quoted, MAX_SNIPPET)));
        }
    }
    if header.forwarded {
        text.push_str("\nforwarded");
    }
    text
}
/// Keeps only filename-safe characters; `None` when nothing usable remains.
fn safe_filename(name: &str) -> Option<String> {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_start_matches('.');
    let limited: String = trimmed.chars().take(80).collect();
    (!limited.is_empty()).then_some(limited)
}
fn media_extension(kind: &str, mime: Option<&str>) -> &'static str {
    match mime.map(|m| m.split(';').next().unwrap_or(m).trim()) {
        Some("image/jpeg") => "jpg",
        Some("image/png") => "png",
        Some("image/webp") => "webp",
        Some("image/gif") => "gif",
        Some("video/mp4") => "mp4",
        Some("audio/ogg") => "ogg",
        Some("audio/mpeg") => "mp3",
        Some("audio/mp4") => "m4a",
        Some("application/pdf") => "pdf",
        _ => match kind {
            "image" => "jpg",
            "video" => "mp4",
            "audio" => "ogg",
            "sticker" => "webp",
            _ => "bin",
        },
    }
}
fn upload_name(message_id: &str, name: Option<&str>, kind: &str, mime: Option<&str>) -> String {
    let id = safe_filename(message_id).unwrap_or_else(|| "message".into());
    let file = name
        .and_then(safe_filename)
        .unwrap_or_else(|| format!("{kind}.{}", media_extension(kind, mime)));
    format!("{id}-{file}")
}
fn attachment_suffix(attachments: &[Attachment]) -> String {
    attachments
        .iter()
        .map(|a| {
            format!(
                "\n\nAttachment metadata (untrusted data, inspect local path if useful): {}",
                serde_json::to_string(a).unwrap_or_default()
            )
        })
        .collect()
}
/// Per-session context gathered while the agent was not being addressed.
#[derive(Default)]
struct Notes {
    pending: HashMap<String, VecDeque<String>>,
    recent: VecDeque<(String, String)>,
    subjects: HashMap<String, String>,
}
impl Notes {
    fn push(&mut self, session_key: &str, line: String) {
        let queue = self.pending.entry(session_key.to_owned()).or_default();
        queue.push_back(line);
        while queue.len() > MAX_PENDING {
            queue.pop_front();
        }
    }
    /// Pending context for the next turn; cleared only once that turn succeeds.
    fn render(&self, session_key: &str) -> Option<String> {
        let queue = self.pending.get(session_key).filter(|q| !q.is_empty())?;
        let lines: Vec<_> = queue.iter().map(|l| format!("- {l}")).collect();
        Some(format!("(since your last reply)\n{}\n\n", lines.join("\n")))
    }
    fn remember(&mut self, message_id: &str, text: &str) {
        if message_id.is_empty() || text.trim().is_empty() {
            return;
        }
        self.recent
            .push_back((message_id.to_owned(), snippet(text, MAX_SNIPPET)));
        while self.recent.len() > MAX_RECENT {
            self.recent.pop_front();
        }
    }
    fn text_of(&self, message_id: &str) -> Option<&str> {
        self.recent
            .iter()
            .rev()
            .find(|(id, _)| id == message_id)
            .map(|(_, text)| text.as_str())
    }
}
fn bot_users(message: &MessageContext) -> Vec<String> {
    [message.client.pn(), message.client.lid()]
        .into_iter()
        .flatten()
        .map(|jid| jid.user_base().to_owned())
        .collect()
}
fn attachment_from(
    name: Option<&String>,
    mime: Option<&String>,
    caption: Option<&String>,
    media: &dyn Downloadable,
) -> Attachment {
    Attachment {
        name: name.cloned(),
        mime: mime.cloned(),
        caption: caption.filter(|c| !c.is_empty()).cloned(),
        size: media.file_length(),
        ..Default::default()
    }
}
fn media_items(message: &wa::Message) -> Vec<(&'static str, Attachment, &dyn Downloadable)> {
    let base = message.get_base_message();
    let mut items: Vec<(&'static str, Attachment, &dyn Downloadable)> = Vec::new();
    if let Some(m) = base.image_message.as_option() {
        items.push((
            "image",
            attachment_from(None, m.mimetype.as_ref(), m.caption.as_ref(), m),
            m,
        ));
    }
    if let Some(m) = base.video_message.as_option() {
        items.push((
            "video",
            attachment_from(None, m.mimetype.as_ref(), m.caption.as_ref(), m),
            m,
        ));
    }
    if let Some(m) = base.audio_message.as_option() {
        items.push((
            "audio",
            attachment_from(None, m.mimetype.as_ref(), None, m),
            m,
        ));
    }
    if let Some(m) = base.document_message.as_option() {
        items.push((
            "document",
            attachment_from(
                m.file_name.as_ref(),
                m.mimetype.as_ref(),
                m.caption.as_ref(),
                m,
            ),
            m,
        ));
    }
    if let Some(m) = base.sticker_message.as_option() {
        items.push((
            "sticker",
            attachment_from(None, m.mimetype.as_ref(), None, m),
            m,
        ));
    }
    items
}
async fn save_media(
    client: &whatsapp_rust::Client,
    media: &dyn Downloadable,
    kind: &str,
    mut attachment: Attachment,
    uploads: &Path,
    message_id: &str,
) -> Attachment {
    let too_large = "larger than 25 MiB".to_owned();
    // The library buffers whole downloads, so an unknown size is not risked.
    match attachment.size {
        Some(size) if size <= MAX_ATTACHMENT_BYTES => {}
        Some(_) => {
            attachment.skipped = Some(too_large);
            return attachment;
        }
        None => {
            attachment.skipped = Some("unknown size".into());
            return attachment;
        }
    }
    let bytes = match tokio::time::timeout(Duration::from_secs(60), client.download(media)).await {
        Ok(Ok(bytes)) if bytes.len() as u64 <= MAX_ATTACHMENT_BYTES => bytes,
        Ok(Ok(_)) => {
            attachment.skipped = Some(too_large);
            return attachment;
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "whatsapp-web media download failed");
            attachment.skipped = Some("download failed".into());
            return attachment;
        }
        Err(_) => {
            attachment.skipped = Some("download timed out".into());
            return attachment;
        }
    };
    let file = upload_name(
        message_id,
        attachment.name.as_deref(),
        kind,
        attachment.mime.as_deref(),
    );
    let written = async {
        tokio::fs::create_dir_all(uploads).await?;
        tokio::fs::write(uploads.join(&file), &bytes).await
    }
    .await;
    match written {
        Ok(()) => {
            attachment.size = Some(bytes.len() as u64);
            attachment.path = Some(format!("uploads/{file}"));
        }
        Err(error) => {
            tracing::warn!(%error, "whatsapp-web media write failed");
            attachment.skipped = Some("could not save file".into());
        }
    }
    attachment
}
async fn sender_phone(message: &MessageContext) -> Option<String> {
    let source = &message.info.source;
    if source.sender.is_pn() {
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
    .filter(|phone| digits(phone))
}
fn reply_of(message: &wa::Message, bot: &[String]) -> Option<Reply> {
    let ctx = message.context_info()?;
    let quoted = ctx.quoted_message.as_option();
    if ctx.stanza_id.is_none() && quoted.is_none() {
        return None;
    }
    Some(Reply {
        who: ctx
            .participant
            .as_deref()
            .map(|p| describe_participant(p, bot)),
        text: quoted
            .and_then(|q| q.text_content().or_else(|| q.get_caption()))
            .map(str::to_owned),
    })
}
async fn from_message(message: &MessageContext) -> Option<Inbound> {
    let source = &message.info.source;
    if source.is_from_me
        || !(source.is_group || source.chat.is_pn() || source.chat.is_lid())
        || message.info.edit != whatsapp_rust::types::message::EditAttribute::Empty
        || whatsapp_rust::types::message::EditAttribute::infer_from_message(&message.message)
            .is_some()
    {
        return None;
    }
    let base = message.message.get_base_message();
    let reaction = base.reaction_message.as_option().map(|r| {
        (
            r.text.clone().unwrap_or_default().trim().to_owned(),
            r.key
                .as_option()
                .and_then(|k| k.id.clone())
                .unwrap_or_default(),
        )
    });
    let text = message
        .message
        .text_content()
        .or_else(|| message.message.get_caption())
        .unwrap_or_default()
        .trim()
        .to_owned();
    let has_media = !media_items(&message.message).is_empty();
    let bot = bot_users(message);
    let mentioned = message
        .message
        .context_info()
        .is_some_and(|ctx| mentions_bot(&ctx.mentioned_jid, &bot));
    let kind = match reaction {
        Some((emoji, _)) if emoji.is_empty() => return None,
        Some((emoji, target)) => Kind::Reaction { emoji, target },
        None => route(source.is_group, mentioned, !text.is_empty() || has_media)?,
    };
    let phone = sender_phone(message).await;
    let group = source
        .is_group
        .then(|| source.chat.user_base().to_owned())
        .filter(|id| !id.is_empty());
    if source.is_group && group.is_none() {
        return None;
    }
    let session_key = if let Some(group) = &group {
        format!("group-{group}")
    } else if let Some(phone) = &phone {
        format!("pn-{phone}")
    } else if source.sender.is_lid() && digits(source.sender.user_base()) {
        format!("lid-{}", source.sender.user_base())
    } else {
        return None;
    };
    let header = Header {
        group: group.clone(),
        name: message.info.push_name.to_string(),
        lid: source
            .sender
            .is_lid()
            .then(|| source.sender.user_base().to_owned()),
        phone: phone.clone(),
        time: format_time(&message.info.timestamp.with_timezone(&chrono::Local)),
        reply: reply_of(&message.message, &bot),
        forwarded: message.message.is_forwarded(),
    };
    Some(Inbound {
        session_key,
        phone,
        group,
        message_id: message.info.id.to_string(),
        kind,
        header,
        text,
    })
}
trait Transport: Clone + Send + Sync + 'static {
    async fn read(&self) -> Result<()>;
    async fn typing(&self, active: bool) -> Result<()>;
    /// Sends a reply quoting the inbound message and returns the sent message id.
    async fn send(&self, text: String) -> Result<String>;
    async fn group_subject(&self) -> Option<String>;
    /// Downloads inbound media into `uploads`; failures become skipped entries.
    async fn attachments(&self, uploads: &Path, message_id: &str) -> Vec<Attachment>;
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
    async fn send(&self, text: String) -> Result<String> {
        Ok(self.reply_quoting(text).await?.message_id.to_string())
    }
    async fn group_subject(&self) -> Option<String> {
        let metadata = self
            .client
            .groups()
            .fetch_metadata(&self.info.source.chat)
            .await
            .ok()?;
        metadata
            .subject
            .map(|s| snippet(&s, 100).replace('"', "'"))
            .filter(|s| !s.is_empty())
    }
    async fn attachments(&self, uploads: &Path, message_id: &str) -> Vec<Attachment> {
        let mut saved = Vec::new();
        for (kind, attachment, media) in
            media_items(&self.message).into_iter().take(MAX_ATTACHMENTS)
        {
            saved
                .push(save_media(&self.client, media, kind, attachment, uploads, message_id).await);
        }
        saved
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
    let mut notes = Notes::default();
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
                            process(&ctx, &cfg, &root, inbound, message, &mut sessions, &mut notes).await
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
    notes: &mut Notes,
) -> Result<()> {
    if !allowed(cfg, inbound.phone.as_deref(), inbound.group.as_deref()) {
        return Ok(());
    }
    let label = sender_label(&inbound.header);
    match &inbound.kind {
        Kind::Reaction { emoji, target } => {
            let quoted = notes
                .text_of(target)
                .map_or_else(|| format!("message {target}"), |t| format!("\"{t}\""));
            notes.push(
                &inbound.session_key,
                format!("{label} reacted {emoji} to {quoted}"),
            );
            return Ok(());
        }
        Kind::Unaddressed => {
            notes.remember(&inbound.message_id, &inbound.text);
            let text = if inbound.text.is_empty() {
                "[media]"
            } else {
                &inbound.text
            };
            let line = format!(
                "{} {label}: {}",
                inbound.header.time,
                snippet(text, MAX_SNIPPET)
            );
            notes.push(&inbound.session_key, line);
            return Ok(());
        }
        Kind::Turn => notes.remember(&inbound.message_id, &inbound.text),
    }
    let sender = match (&inbound.phone, &inbound.header.lid) {
        (Some(phone), _) => format!("phone {phone}"),
        (None, Some(lid)) => format!("lid {lid}"),
        _ => "unknown sender".into(),
    };
    let place = inbound
        .group
        .as_ref()
        .map(|group| format!(" in group {group}"))
        .unwrap_or_default();
    dar_extension_sdk::log::event(
        "-",
        "whatsapp-web",
        &format!("message from {sender}{place}"),
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), transport.read()).await;
    let session_dir = root.join(&inbound.session_key);
    std::fs::create_dir_all(&session_dir)?;
    let subject = match &inbound.group {
        Some(group) => {
            if !notes.subjects.contains_key(group) {
                // Failures are cached as empty so a broken lookup is not retried every turn.
                let subject =
                    tokio::time::timeout(Duration::from_secs(5), transport.group_subject())
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                notes.subjects.insert(group.clone(), subject);
            }
            notes.subjects.get(group).filter(|s| !s.is_empty()).cloned()
        }
        None => None,
    };
    let attachments = transport
        .attachments(&session_dir.join("uploads"), &inbound.message_id)
        .await;
    let mut prompt = notes.render(&inbound.session_key).unwrap_or_default();
    prompt.push_str(&header_text(&inbound.header, subject.as_deref()));
    if !inbound.text.is_empty() {
        prompt.push('\n');
        prompt.push_str(&inbound.text);
    }
    prompt.push_str(&attachment_suffix(&attachments));
    if !sessions.contains_key(&inbound.session_key) {
        let backend_id =
            dar_extension_sdk::chat::resolve_agent_backend(ctx, cfg.backend.as_deref());
        let backend = ctx
            .host
            .services
            .get::<dyn ChatBackend>(&backend_id)
            .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
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
        let turn = tokio::time::timeout(Duration::from_secs(300), turn(connection, prompt));
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
    if result.is_ok() {
        notes.pending.remove(&inbound.session_key);
    } else if let Some(connection) = sessions.remove(&inbound.session_key) {
        let _ = tokio::time::timeout(Duration::from_secs(2), connection.session.close()).await;
    }
    if let Err(error) = &result {
        dar_extension_sdk::log::event(
            "-",
            "whatsapp-web",
            &format!("turn failed for {}: {error:#}", inbound.session_key),
        );
    }
    let reply = result.unwrap_or_else(|_| "(turn failed)".into());
    if !ctx.shutdown.is_cancelled() && !reply.trim().is_empty() {
        let id = tokio::time::timeout(
            Duration::from_secs(20),
            transport.send(adapt_markdown(&reply)),
        )
        .await
        .context("whatsapp send timed out")??;
        notes.remember(&id, &reply);
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
fn connected_message(phone: Option<&str>) -> String {
    match phone {
        Some(phone) => format!("Connected as +{phone}"),
        None => "Connected".into(),
    }
}
fn pairing_message(event: &Event) -> Option<String> {
    match event {
        Event::PairSuccess(pair) => Some(format!(
            "paired as {} ({}, {})",
            pair.id, pair.business_name, pair.platform
        )),
        Event::PairError(pair) => Some(format!("pairing failed: {}", pair.error)),
        Event::LoggedOut(out) => Some(format!(
            "Logged out by WhatsApp ({:?}); delete .whatsapp-web/session.db and restart to pair again",
            out.reason
        )),
        _ => None,
    }
}
/// QR rows for dark terminals; one log event per row keeps them aligned.
fn qr_rows(code: &str) -> Vec<String> {
    let Ok(qr) = qrcode::QrCode::new(code) else {
        return Vec::new();
    };
    qr.render::<qrcode::render::unicode::Dense1x2>()
        .dark_color(qrcode::render::unicode::Dense1x2::Light)
        .light_color(qrcode::render::unicode::Dense1x2::Dark)
        .build()
        .lines()
        .map(str::to_owned)
        .collect()
}
fn log_qr(code: &str) {
    dar_extension_sdk::log::event(
        "-",
        "whatsapp-web",
        "pairing QR fallback (scan in Linked devices):",
    );
    for row in qr_rows(code) {
        dar_extension_sdk::log::event("-", "whatsapp-web", &row);
    }
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
    static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    fn capture(_: &str, event: &str, message: &str) {
        EVENTS.lock().unwrap().push(format!("{event}: {message}"));
    }
    fn logged(message: &str) -> bool {
        EVENTS
            .lock()
            .unwrap()
            .contains(&format!("whatsapp-web: {message}"))
    }
    #[test]
    fn qr_rows_are_aligned_single_lines() {
        let rows = qr_rows("2@abc,def,ghi,jkl");
        let width = rows[0].chars().count();
        assert!(rows.len() > 10 && width > 20);
        assert!(rows
            .iter()
            .all(|row| row.chars().count() == width && !row.contains('\n')));
    }
    #[test]
    fn connected_names_own_phone() {
        assert_eq!(connected_message(Some("3361")), "Connected as +3361");
        assert_eq!(connected_message(None), "Connected");
    }
    #[test]
    fn pairing_events_are_described() {
        let jid: whatsapp_rust::prelude::Jid = "33612345678@s.whatsapp.net".parse().unwrap();
        let lid: whatsapp_rust::prelude::Jid = "123@lid".parse().unwrap();
        let success = Event::PairSuccess(
            whatsapp_rust::types::events::PairSuccess::builder()
                .id(jid.clone())
                .lid(lid.clone())
                .business_name("Sindi".into())
                .platform("smba".into())
                .build(),
        );
        assert_eq!(
            pairing_message(&success).unwrap(),
            "paired as 33612345678@s.whatsapp.net (Sindi, smba)"
        );
        let error = Event::PairError(
            whatsapp_rust::types::events::PairError::builder()
                .id(jid)
                .lid(lid)
                .business_name(String::new())
                .platform(String::new())
                .error("bad".into())
                .build(),
        );
        assert_eq!(pairing_message(&error).unwrap(), "pairing failed: bad");
    }
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
                if prompt.ends_with("hang") {
                    return std::future::pending().await;
                }
                if prompt.ends_with("fail") {
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
        async fn send(&self, text: String) -> Result<String> {
            self.0.lock().unwrap().push(format!("send:{text}"));
            Ok("out-1".into())
        }
        async fn group_subject(&self) -> Option<String> {
            Some("Family".into())
        }
        async fn attachments(&self, _: &Path, id: &str) -> Vec<Attachment> {
            if id != "photo" {
                return Vec::new();
            }
            vec![Attachment {
                path: Some("uploads/m-photo.jpg".into()),
                ..Default::default()
            }]
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
        async fn send(&self, text: String) -> Result<String> {
            self.0.send(text).await
        }
        async fn group_subject(&self) -> Option<String> {
            None
        }
        async fn attachments(&self, _: &Path, _: &str) -> Vec<Attachment> {
            Vec::new()
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
        async fn send(&self, _: String) -> Result<String> {
            std::future::pending().await
        }
        async fn group_subject(&self) -> Option<String> {
            None
        }
        async fn attachments(&self, _: &Path, _: &str) -> Vec<Attachment> {
            Vec::new()
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
    fn inbound(session_key: String, phone: Option<String>, text: String) -> Inbound {
        Inbound {
            header: Header {
                lid: session_key.strip_prefix("lid-").map(str::to_owned),
                phone: phone.clone(),
                time: "2026-10-06 18:02 +02:00".into(),
                ..Default::default()
            },
            session_key,
            phone,
            group: None,
            message_id: "m1".into(),
            kind: Kind::Turn,
            text,
        }
    }
    fn sent_hello(actions: &[String]) -> bool {
        actions
            .iter()
            .any(|a| a.starts_with("send:") && a.ends_with("hello"))
    }
    #[tokio::test]
    async fn group_turn_uses_group_session_header_and_pending_context() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut notes = Notes::default();
        let mut sessions = HashMap::new();
        let group = |kind: Kind, text: &str| {
            let mut i = inbound("group-1203".into(), Some("3361".into()), text.into());
            i.group = Some("1203".into());
            i.header.group = Some("1203".into());
            i.header.name = "Thinh".into();
            i.kind = kind;
            i.message_id = "photo".into();
            i
        };
        let cfg = Config {
            allowed_groups: vec!["1203".into()],
            ..Default::default()
        };
        for inbound in [
            group(Kind::Unaddressed, "lunch?"),
            group(
                Kind::Reaction {
                    emoji: "👍".into(),
                    target: "photo".into(),
                },
                "",
            ),
            group(Kind::Turn, "@bot hi"),
        ] {
            process(
                &ctx,
                &cfg,
                temp.path(),
                inbound,
                transport.clone(),
                &mut sessions,
                &mut notes,
            )
            .await
            .unwrap();
        }
        let actions = transport.0.lock().unwrap().clone();
        assert_eq!(actions.iter().filter(|a| a.starts_with("send:")).count(), 1);
        let reply = actions.iter().find(|a| a.starts_with("send:")).unwrap();
        assert!(reply.contains("(since your last reply)"));
        assert!(reply.contains("Thinh: lunch?"));
        assert!(reply.contains("Thinh reacted 👍 to \"lunch?\""));
        assert!(reply.contains("[WhatsApp group \"Family\" (1203) · from Thinh · +3361"));
        assert!(reply.contains("uploads/m-photo.jpg"));
        assert!(sessions.contains_key("group-1203"));
        assert!(notes.text_of("out-1").is_some());
        let other = Config {
            allowed_groups: vec!["999".into()],
            ..Default::default()
        };
        let transport = FakeTransport::default();
        process(
            &ctx,
            &other,
            temp.path(),
            group(Kind::Turn, "hi"),
            transport.clone(),
            &mut sessions,
            &mut notes,
        )
        .await
        .unwrap();
        assert!(transport.0.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn group_turn_is_logged_with_group() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let mut i = inbound("group-5599".into(), Some("3377".into()), "hi".into());
        i.group = Some("5599".into());
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            i,
            FakeTransport::default(),
            &mut HashMap::new(),
            &mut Notes::default(),
        )
        .await
        .unwrap();
        assert!(logged("message from phone 3377 in group 5599"));
    }
    #[test]
    fn group_and_user_allowlists() {
        let cfg = Config {
            allowed_users: vec!["1".into()],
            allowed_groups: vec!["g1".into()],
            ..Default::default()
        };
        assert!(allowed(&cfg, Some("1"), None));
        assert!(allowed(&cfg, Some("1"), Some("g1")));
        assert!(!allowed(&cfg, Some("2"), Some("g1")));
        assert!(!allowed(&cfg, Some("1"), Some("g2")));
        assert!(!allowed(&cfg, None, None));
        assert!(allowed(&Config::default(), None, Some("any")));
    }
    #[test]
    fn mention_and_route_decisions() {
        let bot = vec!["111".to_owned(), "222".to_owned()];
        assert!(mentions_bot(&["111@s.whatsapp.net".into()], &bot));
        assert!(mentions_bot(&["x@lid".into(), "222:7@lid".into()], &bot));
        assert!(!mentions_bot(&["333@s.whatsapp.net".into()], &bot));
        assert_eq!(route(false, false, true), Some(Kind::Turn));
        assert_eq!(route(true, true, true), Some(Kind::Turn));
        assert_eq!(route(true, false, true), Some(Kind::Unaddressed));
        assert_eq!(route(false, false, false), None);
    }
    #[test]
    fn pending_buffer_is_bounded() {
        let mut notes = Notes::default();
        for i in 0..25 {
            notes.push("k", format!("line {i}"));
        }
        let rendered = notes.render("k").unwrap();
        assert!(!rendered.contains("line 4\n"));
        assert!(rendered.contains("- line 5\n"));
        assert!(rendered.contains("- line 24\n"));
        for i in 0..250 {
            notes.remember(&format!("id{i}"), "text");
        }
        assert!(notes.text_of("id0").is_none());
        assert_eq!(notes.text_of("id249"), Some("text"));
    }
    #[test]
    fn snippets_are_single_line_and_truncated() {
        assert_eq!(snippet("a\n b  c", 200), "a b c");
        assert_eq!(snippet(&"x".repeat(250), 200).chars().count(), 201);
        assert!(snippet(&"x".repeat(250), 200).ends_with('…'));
    }
    #[test]
    fn header_formats_dm_group_reply_and_forward() {
        let mut header = Header {
            name: "Thinh".into(),
            phone: Some("33695189048".into()),
            time: "2026-10-06 18:02 +02:00".into(),
            ..Default::default()
        };
        assert_eq!(
            header_text(&header, None),
            "[WhatsApp DM · from Thinh · +33695189048 · 2026-10-06 18:02 +02:00]"
        );
        header.group = Some("1203".into());
        header.reply = Some(Reply {
            who: Some("you".into()),
            text: Some("a\nb".into()),
        });
        header.forwarded = true;
        assert_eq!(
            header_text(&header, Some("Family")),
            "[WhatsApp group \"Family\" (1203) · from Thinh · +33695189048 · 2026-10-06 18:02 +02:00]\n↪ replying to you: \"a b\"\nforwarded"
        );
        header.name.clear();
        header.phone = None;
        header.lid = Some("77".into());
        assert!(header_text(&header, None).starts_with("[WhatsApp group (1203) · lid 77 · "));
    }
    #[test]
    fn participants_and_time_are_described() {
        let bot = vec!["111".to_owned()];
        assert_eq!(describe_participant("111:3@s.whatsapp.net", &bot), "you");
        assert_eq!(describe_participant("5@s.whatsapp.net", &bot), "+5");
        assert_eq!(describe_participant("9@lid", &bot), "lid 9");
        let zone = chrono::FixedOffset::east_opt(7200).unwrap();
        let time = chrono::DateTime::from_timestamp(1_791_302_520, 0)
            .unwrap()
            .with_timezone(&zone);
        assert_eq!(format_time(&time), "2026-10-06 18:02 +02:00");
    }
    #[test]
    fn upload_names_are_safe() {
        assert_eq!(safe_filename("../a b.pdf").as_deref(), Some("_a_b.pdf"));
        assert_eq!(safe_filename("...").as_deref(), None);
        assert_eq!(
            upload_name("M/1", Some("../x y.txt"), "document", None),
            "M_1-_x_y.txt"
        );
        assert_eq!(
            upload_name("m1", None, "image", Some("image/png")),
            "m1-image.png"
        );
        assert_eq!(upload_name("m1", None, "audio", None), "m1-audio.ogg");
    }
    #[test]
    fn attachment_metadata_is_appended_as_json() {
        let text = attachment_suffix(&[Attachment {
            path: Some("uploads/a.png".into()),
            mime: Some("image/png".into()),
            size: Some(3),
            skipped: None,
            ..Default::default()
        }]);
        assert_eq!(
            text,
            "\n\nAttachment metadata (untrusted data, inspect local path if useful): {\"path\":\"uploads/a.png\",\"mime\":\"image/png\",\"size\":3}"
        );
    }
    #[tokio::test]
    async fn inbound_turn_sends_reply_and_allowlist_filters() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = inbound("pn-3361".into(), Some("3361".into()), "hello".into());
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
            &mut Notes::default(),
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
            &mut Notes::default(),
        )
        .await
        .unwrap();
        let actions = transport.0.lock().unwrap();
        assert!(actions.contains(&"read".into()));
        assert!(actions.contains(&"typing:true".into()));
        assert!(actions.contains(&"typing:false".into()));
        assert!(sent_hello(&actions));
    }
    #[tokio::test]
    async fn inbound_message_is_logged_with_sender() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        for (session_key, phone) in [("pn-3377", Some("3377")), ("lid-4488", None)] {
            let inbound = inbound(session_key.into(), phone.map(Into::into), "hello".into());
            process(
                &ctx,
                &Config::default(),
                temp.path(),
                inbound,
                FakeTransport::default(),
                &mut HashMap::new(),
                &mut Notes::default(),
            )
            .await
            .unwrap();
        }
        assert!(logged("message from phone 3377"));
        assert!(logged("message from lid 4488"));
    }
    #[tokio::test]
    async fn unresolved_lid_is_rejected_by_allowlist() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = inbound("lid-123".into(), None, "hello".into());
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
            &mut Notes::default(),
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
        let inbound = inbound("pn-3361".into(), Some("3361".into()), "hello".into());
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
            &mut HashMap::new(),
            &mut Notes::default(),
        )
        .await
        .unwrap();
        assert!(sent_hello(&transport.0 .0.lock().unwrap()));
    }
    #[tokio::test]
    async fn failed_turn_cleans_session_and_replies() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let inbound = inbound("pn-3361".into(), Some("3361".into()), "fail".into());
        let mut sessions = HashMap::new();
        let mut notes = Notes::default();
        notes.push("pn-3361", "Ana reacted 👍".into());
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            inbound.clone(),
            transport.clone(),
            &mut sessions,
            &mut notes,
        )
        .await
        .unwrap();
        assert!(sessions.is_empty());
        assert!(notes.render("pn-3361").is_some(), "kept for retry");
        assert!(logged("turn failed for pn-3361: failed"));
        let ok = Inbound {
            text: "hello".into(),
            ..inbound
        };
        process(
            &ctx,
            &Config::default(),
            temp.path(),
            ok,
            transport.clone(),
            &mut sessions,
            &mut notes,
        )
        .await
        .unwrap();
        assert!(notes.render("pn-3361").is_none(), "cleared after success");
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
            inbound("pn-3361".into(), Some("3361".into()), "hang".into()),
            transport.clone(),
            &mut sessions,
            &mut Notes::default(),
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
            inbound("pn-3361".into(), Some("3361".into()), "hello".into()),
            transport.clone(),
            &mut HashMap::new(),
            &mut Notes::default(),
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
