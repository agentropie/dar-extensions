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
use tokio::{sync::mpsc, task::JoinSet};
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
    sessions: Sessions,
    messages: Messages,
}
/// Chat session lifetime. No `idle_minutes` (or 0) means sessions never expire.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct Sessions {
    idle_minutes: Option<u64>,
}
/// Replies to the `/stop`, `/new` and `/compact` commands.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct Messages {
    stopped: String,
    new_session: String,
    compacted: String,
}
impl Default for Messages {
    fn default() -> Self {
        Self {
            stopped: "Stopped.".into(),
            new_session: "New session started.".into(),
            compacted: "Compacted.".into(),
        }
    }
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
/// Chat command, recognised only when the whole message is the command.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Command {
    New,
    Stop,
    Compact,
}
#[derive(Clone)]
struct Inbound {
    session_key: String,
    phone: Option<String>,
    group: Option<String>,
    message_id: String,
    kind: Kind,
    command: Option<Command>,
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
/// A group command must @-mention the bot; the mention tokens are removed before
/// matching. DMs use the bare command.
fn parse_command(text: &str, is_group: bool, bot_users: &[String]) -> Option<Command> {
    let remaining = if is_group {
        let tokens: Vec<_> = text.split_whitespace().collect();
        let rest: Vec<_> = tokens
            .iter()
            .filter(|token| {
                !bot_users
                    .iter()
                    .any(|bot| token.strip_prefix('@') == Some(bot.as_str()))
            })
            .collect();
        if rest.len() == tokens.len() {
            return None;
        }
        rest.iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        text.trim().to_owned()
    };
    match remaining.as_str() {
        "/new" => Some(Command::New),
        "/stop" => Some(Command::Stop),
        "/compact" => Some(Command::Compact),
        _ => None,
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
    let command = (kind == Kind::Turn && !has_media)
        .then(|| parse_command(&text, source.is_group, &bot))
        .flatten();
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
        command,
        header,
        text,
    })
}
trait Transport: Clone + Send + Sync + 'static {
    fn read(&self) -> impl std::future::Future<Output = Result<()>> + Send;
    fn typing(&self, active: bool) -> impl std::future::Future<Output = Result<()>> + Send;
    /// Sends a reply quoting the inbound message and returns the sent message id.
    fn send(&self, text: String) -> impl std::future::Future<Output = Result<String>> + Send;
    fn group_subject(&self) -> impl std::future::Future<Output = Option<String>> + Send;
    /// Downloads inbound media into `uploads`; failures become skipped entries.
    fn attachments(
        &self,
        uploads: &Path,
        message_id: &str,
    ) -> impl std::future::Future<Output = Vec<Attachment>> + Send;
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
const ACTIVITY_FILE: &str = "last_activity";
const CURRENT_FILE: &str = "current";
const COMPACT_PERCENT: u64 = 80;
static NO_WINDOW_LOGGED: AtomicBool = AtomicBool::new(false);
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| value.as_secs())
}
fn write_atomic(path: &Path, text: &str) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temporary, text)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn read_number(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}
/// Current generation of a chat; a chat without a `current` pointer is generation 1.
fn current_generation(chat_dir: &Path) -> u64 {
    read_number(&chat_dir.join(CURRENT_FILE))
        .filter(|generation| *generation > 0)
        .unwrap_or(1)
}
fn generation_dir(chat_dir: &Path) -> Result<std::path::PathBuf> {
    let dir = chat_dir.join(current_generation(chat_dir).to_string());
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
/// Starts a new generation directory; the previous one stays on disk.
fn rotate_generation(chat_dir: &Path) -> Result<()> {
    let mut next = current_generation(chat_dir) + 1;
    while chat_dir.join(next.to_string()).exists() {
        next += 1;
    }
    std::fs::create_dir_all(chat_dir.join(next.to_string()))?;
    write_atomic(&chat_dir.join(CURRENT_FILE), &next.to_string())
}
fn touch_activity(chat_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(chat_dir)?;
    write_atomic(&chat_dir.join(ACTIVITY_FILE), &now_secs().to_string())
}
/// Idle minutes elapsed since the last recorded activity, when it reaches the TTL.
fn idle_expired(chat_dir: &Path, idle_minutes: Option<u64>) -> Option<u64> {
    let minutes = idle_minutes.filter(|minutes| *minutes > 0)?;
    let last = read_number(&chat_dir.join(ACTIVITY_FILE))?;
    (now_secs().saturating_sub(last) >= minutes.saturating_mul(60)).then_some(minutes)
}
fn over_compact_threshold(tokens_used: u64, context_window: u64) -> bool {
    context_window > 0
        && u128::from(tokens_used) * 100 >= u128::from(COMPACT_PERCENT) * u128::from(context_window)
}
struct Connection {
    session: Box<dyn dar_extension_sdk::chat::ChatSession>,
    events: mpsc::Receiver<ChatEvent>,
    usage: Option<(u64, u64)>,
    /// Auto-compaction may fire; cleared when it fires, set by a later usage below the threshold.
    armed: bool,
    /// The backend session is unusable and must be dropped and reopened.
    closed: bool,
}
impl Connection {
    fn record_usage(&mut self, tokens_used: u64, context_window: Option<u64>) {
        let Some(window) = context_window else {
            if !NO_WINDOW_LOGGED.swap(true, Ordering::Relaxed) {
                dar_extension_sdk::log::event(
                    "-",
                    "whatsapp-web",
                    "Auto-compaction unavailable: backend did not report a context window",
                );
            }
            // An earlier windowed report is stale once a newer one lacks the window.
            self.usage = None;
            return;
        };
        self.usage = Some((tokens_used, window));
        if !over_compact_threshold(tokens_used, window) {
            self.armed = true;
        }
    }
    /// Percent of the context used, when auto-compaction should fire now.
    fn compact_percent(&self) -> Option<u64> {
        let (tokens_used, window) = self.usage?;
        (self.armed && over_compact_threshold(tokens_used, window))
            .then(|| tokens_used.saturating_mul(100) / window)
    }
}
struct TurnOutcome {
    result: Result<String>,
    stopped: bool,
}
/// State of one chat, owned by that chat's worker task.
struct Chat<T> {
    key: String,
    connection: Option<Connection>,
    notes: Notes,
    rx: mpsc::UnboundedReceiver<(Inbound, T)>,
    /// Messages that arrived during a turn and are processed after it, in order.
    deferred: VecDeque<(Inbound, T)>,
}
/// Sends a command or turn reply; failures are logged, never fatal.
async fn say<T: Transport>(transport: &T, text: &str) -> Option<String> {
    match tokio::time::timeout(
        Duration::from_secs(20),
        transport.send(adapt_markdown(text)),
    )
    .await
    {
        Ok(Ok(id)) => Some(id),
        Ok(Err(error)) => {
            tracing::warn!(%error, "whatsapp-web reply failed");
            None
        }
        Err(_) => {
            tracing::warn!("whatsapp-web reply timed out");
            None
        }
    }
}
impl<T: Transport> Chat<T> {
    fn new(key: String, rx: mpsc::UnboundedReceiver<(Inbound, T)>) -> Self {
        Self {
            key,
            connection: None,
            notes: Notes::default(),
            rx,
            deferred: VecDeque::new(),
        }
    }
    async fn close_connection(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), connection.session.close()).await;
        }
    }
    /// Opens the backend session if needed, resuming the newest one in `dir`.
    async fn ensure_open(&mut self, ctx: &StartCtx, cfg: &Config, dir: &Path) -> Result<()> {
        if self.connection.is_some() {
            return Ok(());
        }
        let backend_id =
            dar_extension_sdk::chat::resolve_agent_backend(ctx, cfg.backend.as_deref());
        let backend = ctx
            .host
            .services
            .get::<dyn ChatBackend>(&backend_id)
            .with_context(|| format!("chat backend '{backend_id}' not registered"))?;
        let (tx, events) = mpsc::channel(256);
        let params = dar_extension_sdk::chat::agent_session_params(ctx, dir)
            .resume_session_id(dar_extension_sdk::chat::archive::newest_session_id(
                dir,
                &backend_id,
            ))
            .build();
        let session = tokio::time::timeout(Duration::from_secs(30), backend.open(params, tx))
            .await
            .context("chat backend open timed out")??;
        self.connection = Some(Connection {
            session,
            events,
            usage: None,
            armed: true,
            closed: false,
        });
        Ok(())
    }
    /// Applies the idle TTL, records activity and returns the current generation dir.
    async fn begin(&mut self, root: &Path, cfg: &Config) -> Result<std::path::PathBuf> {
        let chat_dir = root.join(&self.key);
        std::fs::create_dir_all(&chat_dir)?;
        if let Some(minutes) = idle_expired(&chat_dir, cfg.sessions.idle_minutes) {
            self.close_connection().await;
            rotate_generation(&chat_dir)?;
            dar_extension_sdk::log::event(
                "-",
                "whatsapp-web",
                &format!(
                    "Session {} expired after {minutes} min idle; starting fresh",
                    self.key
                ),
            );
        }
        touch_activity(&chat_dir)?;
        generation_dir(&chat_dir)
    }
    /// Runs one backend turn. A `/stop` arriving meanwhile aborts it; any other
    /// message waits in `deferred`.
    async fn run_turn(
        &mut self,
        cfg: &Config,
        prompt: String,
        transport: &T,
        typing: bool,
    ) -> TurnOutcome {
        let Some(connection) = self.connection.as_mut() else {
            return TurnOutcome {
                result: Err(anyhow::anyhow!("session missing")),
                stopped: false,
            };
        };
        let rx = &mut self.rx;
        let deferred = &mut self.deferred;
        let mut rx_open = true;
        let mut stopped = false;
        if typing {
            let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(true)).await;
        }
        let mut tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
        );
        let result = tokio::time::timeout(Duration::from_secs(300), async {
            if let Err(error) = connection.session.send_turn(prompt).await {
                connection.closed = true;
                return Err(error);
            }
            let mut reply = String::new();
            let mut silent = false;
            loop {
                tokio::select! {
                    event = connection.events.recv() => {
                        let Some(event) = event else {
                            connection.closed = true;
                            bail!("backend event stream closed")
                        };
                        match event {
                            ChatEvent::Delta { role: ChatRole::Assistant, text } if !silent => {
                                reply.push_str(&text)
                            }
                            ChatEvent::Silent { .. } => {
                                silent = true;
                                reply.clear();
                            }
                            ChatEvent::ContextUsage { tokens_used, context_window } => {
                                connection.record_usage(tokens_used, context_window)
                            }
                            ChatEvent::TurnFinished { ok: true, .. } => return Ok(reply),
                            ChatEvent::TurnFinished { error, .. } => {
                                bail!("{}", error.unwrap_or_else(|| "backend failed".into()))
                            }
                            ChatEvent::SessionClosed { error } => {
                                connection.closed = true;
                                bail!("{}", error.unwrap_or_else(|| "backend failed".into()))
                            }
                            _ => {}
                        }
                    }
                    message = rx.recv(), if rx_open => match message {
                        None => rx_open = false,
                        Some((inbound, reply_to))
                            if inbound.command == Some(Command::Stop)
                                && allowed(cfg, inbound.phone.as_deref(), inbound.group.as_deref()) =>
                        {
                            stopped = true;
                            if let Err(error) = connection.session.abort().await {
                                tracing::warn!(%error, "whatsapp-web abort failed");
                            }
                            say(&reply_to, &cfg.messages.stopped).await;
                        }
                        Some(item) => deferred.push_back(item),
                    },
                    _ = tick.tick(), if typing => {
                        let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(true)).await;
                    }
                }
            }
        })
        .await;
        let result = result.unwrap_or_else(|_| {
            connection.closed = true;
            Err(anyhow::anyhow!("chat turn timed out"))
        });
        if typing {
            let _ = tokio::time::timeout(Duration::from_secs(5), transport.typing(false)).await;
        }
        if connection.closed {
            self.close_connection().await;
        }
        TurnOutcome { result, stopped }
    }
    /// Sends `/compact` as a turn and discards the backend's text.
    async fn compact(&mut self, cfg: &Config, root: &Path, transport: &T, typing: bool) -> bool {
        let outcome = self
            .run_turn(cfg, "/compact".into(), transport, typing)
            .await;
        let _ = touch_activity(&root.join(&self.key));
        match outcome.result {
            Ok(_) => {
                dar_extension_sdk::log::event(
                    "-",
                    "whatsapp-web",
                    &format!("Compacted {}", self.key),
                );
                true
            }
            Err(error) => {
                if !outcome.stopped {
                    dar_extension_sdk::log::event(
                        "-",
                        "whatsapp-web",
                        &format!("Compaction failed for {}: {error:#}", self.key),
                    );
                }
                false
            }
        }
    }
}
/// One worker task per chat key; a chat's messages stay ordered, chats run concurrently.
struct Workers<T: Transport> {
    ctx: StartCtx,
    cfg: Config,
    root: std::path::PathBuf,
    senders: HashMap<String, mpsc::UnboundedSender<(Inbound, T)>>,
    tasks: JoinSet<()>,
    /// Tells workers to close their sessions and exit, independent of host shutdown.
    stop: tokio::sync::watch::Sender<bool>,
}
impl<T: Transport> Workers<T> {
    fn new(ctx: StartCtx, cfg: Config, root: std::path::PathBuf) -> Self {
        Self {
            ctx,
            cfg,
            root,
            senders: HashMap::new(),
            tasks: JoinSet::new(),
            stop: tokio::sync::watch::channel(false).0,
        }
    }
    fn submit(&mut self, inbound: Inbound, transport: T) {
        if !allowed(
            &self.cfg,
            inbound.phone.as_deref(),
            inbound.group.as_deref(),
        ) {
            return;
        }
        let mut item = (inbound, transport);
        if let Some(sender) = self.senders.get(&item.0.session_key) {
            match sender.send(item) {
                Ok(()) => return,
                Err(returned) => item = returned.0,
            }
        }
        let key = item.0.session_key.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = tx.send(item);
        self.senders.insert(key.clone(), tx);
        self.tasks.spawn(chat_worker(
            self.ctx.clone(),
            self.cfg.clone(),
            self.root.clone(),
            Chat::new(key, rx),
            self.stop.subscribe(),
        ));
    }
    /// Lets workers close their sessions, then aborts any that overrun `grace`.
    async fn finish(mut self, grace: Duration) {
        self.senders.clear();
        let _ = self.stop.send(true);
        let joined = tokio::time::timeout(grace, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        if joined.is_err() {
            self.tasks.abort_all();
        }
    }
}
async fn chat_worker<T: Transport>(
    ctx: StartCtx,
    cfg: Config,
    root: std::path::PathBuf,
    mut chat: Chat<T>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut shutdown = ctx.shutdown.clone();
    loop {
        let next = match chat.deferred.pop_front() {
            Some(item) => item,
            None => tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = stop.changed() => break,
                item = chat.rx.recv() => match item {
                    Some(item) => item,
                    None => break,
                },
            },
        };
        let result = tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = stop.changed() => break,
            result = process(&mut chat, &ctx, &cfg, &root, next.0, next.1) => result,
        };
        if let Err(error) = result {
            tracing::warn!(%error, "whatsapp-web inbound turn failed");
        }
    }
    if let Some(connection) = chat.connection.take() {
        let _ = tokio::time::timeout(Duration::from_millis(100), connection.session.close()).await;
    }
}
async fn dispatch(
    ctx: StartCtx,
    cfg: Config,
    root: std::path::PathBuf,
    mut rx: mpsc::Receiver<MessageContext>,
    bot: Bot,
) {
    let mut handle = bot.spawn();
    let mut workers = Workers::new(ctx.clone(), cfg, root);
    let mut shutdown = ctx.shutdown.clone();
    let mut completed = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            outcome = &mut handle => { tracing::warn!(?outcome, "whatsapp-web connection stopped"); completed = true; break; },
            message = rx.recv() => {
                let Some(message) = message else { break; };
                if let Some(inbound) = from_message(&message).await {
                    workers.submit(inbound, message);
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
    workers.finish(Duration::from_millis(300)).await;
}
async fn process<T: Transport>(
    chat: &mut Chat<T>,
    ctx: &StartCtx,
    cfg: &Config,
    root: &Path,
    inbound: Inbound,
    transport: T,
) -> Result<()> {
    if !allowed(cfg, inbound.phone.as_deref(), inbound.group.as_deref()) {
        return Ok(());
    }
    let label = sender_label(&inbound.header);
    match &inbound.kind {
        Kind::Reaction { emoji, target } => {
            let quoted = chat
                .notes
                .text_of(target)
                .map_or_else(|| format!("message {target}"), |t| format!("\"{t}\""));
            chat.notes.push(
                &inbound.session_key,
                format!("{label} reacted {emoji} to {quoted}"),
            );
            return Ok(());
        }
        Kind::Unaddressed => {
            chat.notes.remember(&inbound.message_id, &inbound.text);
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
            chat.notes.push(&inbound.session_key, line);
            return Ok(());
        }
        Kind::Turn => {}
    }
    if let Some(command) = inbound.command {
        return run_command(chat, ctx, cfg, root, command, &transport).await;
    }
    chat.notes.remember(&inbound.message_id, &inbound.text);
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
    let session_dir = chat.begin(root, cfg).await?;
    let subject = match &inbound.group {
        Some(group) => {
            if !chat.notes.subjects.contains_key(group) {
                // Failures are cached as empty so a broken lookup is not retried every turn.
                let subject =
                    tokio::time::timeout(Duration::from_secs(5), transport.group_subject())
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                chat.notes.subjects.insert(group.clone(), subject);
            }
            chat.notes
                .subjects
                .get(group)
                .filter(|s| !s.is_empty())
                .cloned()
        }
        None => None,
    };
    let attachments = transport
        .attachments(&session_dir.join("uploads"), &inbound.message_id)
        .await;
    let mut prompt = chat.notes.render(&inbound.session_key).unwrap_or_default();
    prompt.push_str(&header_text(&inbound.header, subject.as_deref()));
    if !inbound.text.is_empty() {
        prompt.push('\n');
        prompt.push_str(&inbound.text);
    }
    prompt.push_str(&attachment_suffix(&attachments));
    chat.ensure_open(ctx, cfg, &session_dir).await?;
    let outcome = chat.run_turn(cfg, prompt, &transport, true).await;
    if outcome.result.is_ok() {
        chat.notes.pending.remove(&inbound.session_key);
    }
    if let Err(error) = &outcome.result {
        if !outcome.stopped {
            dar_extension_sdk::log::event(
                "-",
                "whatsapp-web",
                &format!("turn failed for {}: {error:#}", inbound.session_key),
            );
        }
    }
    let _ = touch_activity(&root.join(&chat.key));
    let succeeded = outcome.result.is_ok() && !outcome.stopped;
    if !outcome.stopped {
        let reply = outcome.result.unwrap_or_else(|_| "(turn failed)".into());
        if !ctx.shutdown.is_cancelled() && !reply.trim().is_empty() {
            let id = tokio::time::timeout(
                Duration::from_secs(20),
                transport.send(adapt_markdown(&reply)),
            )
            .await
            .context("whatsapp send timed out")??;
            chat.notes.remember(&id, &reply);
        }
    }
    let percent = chat
        .connection
        .as_ref()
        .and_then(Connection::compact_percent)
        .filter(|_| succeeded);
    if let Some(percent) = percent {
        if let Some(connection) = chat.connection.as_mut() {
            connection.armed = false;
        }
        dar_extension_sdk::log::event(
            "-",
            "whatsapp-web",
            &format!("Auto-compacting {} ({percent}% of context)", chat.key),
        );
        chat.compact(cfg, root, &transport, false).await;
    }
    Ok(())
}
async fn run_command<T: Transport>(
    chat: &mut Chat<T>,
    ctx: &StartCtx,
    cfg: &Config,
    root: &Path,
    command: Command,
    transport: &T,
) -> Result<()> {
    match command {
        Command::New => {
            chat.close_connection().await;
            let chat_dir = root.join(&chat.key);
            std::fs::create_dir_all(&chat_dir)?;
            rotate_generation(&chat_dir)?;
            touch_activity(&chat_dir)?;
            let key = chat.key.clone();
            chat.notes.pending.remove(&key);
            say(transport, &cfg.messages.new_session).await;
        }
        // Nothing is in flight when a stop is handled here; in-flight turns are stopped by `run_turn`.
        Command::Stop => {
            say(transport, &cfg.messages.stopped).await;
        }
        Command::Compact => {
            let dir = chat.begin(root, cfg).await?;
            if let Err(error) = chat.ensure_open(ctx, cfg, &dir).await {
                dar_extension_sdk::log::event(
                    "-",
                    "whatsapp-web",
                    &format!("Compaction failed for {}: {error:#}", chat.key),
                );
                return Ok(());
            }
            if chat.compact(cfg, root, transport, true).await {
                say(transport, &cfg.messages.compacted).await;
            }
        }
    }
    Ok(())
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
    use tokio::sync::{watch, Notify};
    /// What the fake backend saw: resume ids per open, and every prompt sent.
    #[derive(Clone, Default)]
    struct Probe {
        opens: Arc<Mutex<Vec<Option<String>>>>,
        prompts: Arc<Mutex<Vec<String>>>,
        closes: Arc<Mutex<usize>>,
    }
    impl Probe {
        fn opens(&self) -> Vec<Option<String>> {
            self.opens.lock().unwrap().clone()
        }
        fn compactions(&self) -> usize {
            self.prompts
                .lock()
                .unwrap()
                .iter()
                .filter(|p| *p == "/compact")
                .count()
        }
    }
    struct EchoBackend(Probe);
    struct EchoSession {
        events: mpsc::Sender<ChatEvent>,
        probe: Probe,
        abort: Arc<Notify>,
    }
    impl ChatBackend for EchoBackend {
        fn open<'a>(
            &'a self,
            params: ChatSessionParams,
            events: mpsc::Sender<ChatEvent>,
        ) -> BoxFuture<'a, Result<Box<dyn ChatSession>>> {
            self.0
                .opens
                .lock()
                .unwrap()
                .push(params.resume_session_id.clone());
            let probe = self.0.clone();
            Box::pin(async move {
                Ok(Box::new(EchoSession {
                    events,
                    probe,
                    abort: Arc::new(Notify::new()),
                }) as Box<dyn ChatSession>)
            })
        }
    }
    impl ChatSession for EchoSession {
        fn send_turn(&mut self, prompt: String) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.probe.prompts.lock().unwrap().push(prompt.clone());
                if prompt.ends_with("hang") {
                    return std::future::pending().await;
                }
                if prompt.ends_with("slow") {
                    let (events, abort) = (self.events.clone(), self.abort.clone());
                    tokio::spawn(async move {
                        let _ = events
                            .send(ChatEvent::Delta {
                                role: ChatRole::Assistant,
                                text: "partial".into(),
                            })
                            .await;
                        abort.notified().await;
                        let _ = events
                            .send(ChatEvent::TurnFinished {
                                ok: false,
                                error: Some("aborted".into()),
                            })
                            .await;
                    });
                    return Ok(());
                }
                if prompt.ends_with("close") {
                    self.events
                        .send(ChatEvent::SessionClosed {
                            error: Some("died".into()),
                        })
                        .await?;
                    return Ok(());
                }
                if prompt.ends_with("fail") {
                    self.events
                        .send(ChatEvent::TurnFinished {
                            ok: false,
                            error: Some("failed".into()),
                        })
                        .await?;
                    return Ok(());
                }
                if prompt.ends_with("stale") {
                    self.events
                        .send(ChatEvent::ContextUsage {
                            tokens_used: 85,
                            context_window: Some(100),
                        })
                        .await?;
                }
                let usage = [
                    ("big", Some(100)),
                    ("edge", Some(100)),
                    ("under", Some(100)),
                    ("small", Some(100)),
                    ("nowin", None),
                    ("stale", None),
                ]
                .into_iter()
                .find(|(word, _)| prompt.ends_with(word));
                if let Some((word, window)) = usage {
                    let tokens_used = match word {
                        "big" => 85,
                        "edge" => 80,
                        "under" => 79,
                        "stale" => 90,
                        _ => 10,
                    };
                    self.events
                        .send(ChatEvent::ContextUsage {
                            tokens_used,
                            context_window: window,
                        })
                        .await?;
                }
                self.events
                    .send(ChatEvent::Delta {
                        role: ChatRole::Assistant,
                        text: if prompt == "/compact" {
                            "compact text".into()
                        } else {
                            prompt
                        },
                    })
                    .await?;
                self.events
                    .send(ChatEvent::TurnFinished {
                        ok: true,
                        error: None,
                    })
                    .await?;
                Ok(())
            })
        }
        fn abort(&mut self) -> BoxFuture<'_, Result<()>> {
            self.abort.notify_one();
            Box::pin(async { Ok(()) })
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            *self.probe.closes.lock().unwrap() += 1;
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
        context_with(root, Probe::default())
    }
    fn context_with(root: &Path, probe: Probe) -> (StartCtx, watch::Sender<bool>) {
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
            .service::<dyn ChatBackend>("pi", Arc::new(EchoBackend(probe)))
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
            command: None,
            text,
        }
    }
    fn command(key: &str, phone: &str, command: Command) -> Inbound {
        Inbound {
            command: Some(command),
            ..inbound(key.into(), Some(phone.into()), String::new())
        }
    }
    fn new_chat<T: Transport>(key: &str) -> Chat<T> {
        Chat::new(key.into(), mpsc::unbounded_channel().1)
    }
    async fn wait_for(what: &str, condition: impl Fn() -> bool) {
        for _ in 0..500 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }
    fn sends(transport: &FakeTransport) -> Vec<String> {
        transport
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.starts_with("send:"))
            .cloned()
            .collect()
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
        let mut chat = new_chat("group-1203");
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
                &mut chat,
                &ctx,
                &cfg,
                temp.path(),
                inbound,
                transport.clone(),
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
        assert!(chat.connection.is_some());
        assert!(chat.notes.text_of("out-1").is_some());
        let other = Config {
            allowed_groups: vec!["999".into()],
            ..Default::default()
        };
        let transport = FakeTransport::default();
        process(
            &mut chat,
            &ctx,
            &other,
            temp.path(),
            group(Kind::Turn, "hi"),
            transport.clone(),
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
            &mut new_chat("group-5599"),
            &ctx,
            &Config::default(),
            temp.path(),
            i,
            FakeTransport::default(),
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
        let mut chat = new_chat("pn-3361");
        let cfg = Config {
            allowed_users: vec!["999".into()],
            ..Default::default()
        };
        process(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            inbound.clone(),
            transport.clone(),
        )
        .await
        .unwrap();
        assert!(transport.0.lock().unwrap().is_empty());
        process(
            &mut chat,
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
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
                &mut new_chat("pn-3361"),
                &ctx,
                &Config::default(),
                temp.path(),
                inbound,
                FakeTransport::default(),
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
            &mut new_chat("pn-3361"),
            &ctx,
            &cfg,
            temp.path(),
            inbound,
            transport.clone(),
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
            &mut new_chat("pn-3361"),
            &ctx,
            &Config::default(),
            temp.path(),
            inbound,
            transport.clone(),
        )
        .await
        .unwrap();
        assert!(sent_hello(&transport.0 .0.lock().unwrap()));
    }
    #[tokio::test(start_paused = true)]
    async fn hung_turn_times_out_and_cleans_session() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-3361");
        let started = tokio::time::Instant::now();
        process(
            &mut chat,
            &ctx,
            &Config::default(),
            temp.path(),
            inbound("pn-3361".into(), Some("3361".into()), "hang".into()),
            transport.clone(),
        )
        .await
        .unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(300));
        assert!(chat.connection.is_none());
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
            &mut new_chat("pn-3361"),
            &ctx,
            &Config::default(),
            temp.path(),
            inbound("pn-3361".into(), Some("3361".into()), "hello".into()),
            transport.clone(),
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
    fn turn_in(key: &str, text: &str) -> Inbound {
        inbound(key.into(), Some("3361".into()), text.into())
    }
    async fn run<T: Transport>(
        chat: &mut Chat<T>,
        ctx: &StartCtx,
        cfg: &Config,
        root: &Path,
        inbound: Inbound,
        transport: &T,
    ) {
        process(chat, ctx, cfg, root, inbound, transport.clone())
            .await
            .unwrap();
    }
    fn backdate_activity(root: &Path, key: &str, minutes: u64) {
        let last = now_secs() - minutes * 60;
        std::fs::write(root.join(key).join(ACTIVITY_FILE), last.to_string()).unwrap();
    }
    #[tokio::test]
    async fn reopen_resumes_newest_archived_session() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let generation = temp.path().join("pn-3361").join("1");
        std::fs::create_dir_all(&generation).unwrap();
        std::fs::write(
            generation.join("2026-01-01_a.jsonl"),
            "{\"type\":\"session\",\"id\":\"sess-7\",\"backend\":\"pi\"}\n",
        )
        .unwrap();
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-3361");
        run(
            &mut chat,
            &ctx,
            &Config::default(),
            temp.path(),
            turn_in("pn-3361", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens(), vec![Some("sess-7".to_owned())]);
        let mut other = new_chat("pn-9");
        run(
            &mut other,
            &ctx,
            &Config::default(),
            temp.path(),
            turn_in("pn-9", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens()[1], None);
    }
    #[tokio::test]
    async fn legacy_chat_dir_without_generations_is_generation_one() {
        let temp = tempfile::tempdir().unwrap();
        let chat_dir = temp.path().join("pn-1");
        std::fs::create_dir_all(chat_dir.join("uploads")).unwrap();
        assert_eq!(current_generation(&chat_dir), 1);
        assert_eq!(generation_dir(&chat_dir).unwrap(), chat_dir.join("1"));
        rotate_generation(&chat_dir).unwrap();
        assert_eq!(current_generation(&chat_dir), 2);
        assert!(chat_dir.join("1").is_dir() && chat_dir.join("2").is_dir());
    }
    #[tokio::test]
    async fn idle_ttl_rotates_generation_and_reopens() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let cfg = Config {
            sessions: Sessions {
                idle_minutes: Some(60),
            },
            ..Default::default()
        };
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-ttl");
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-ttl", "hello"),
            &transport,
        )
        .await;
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-ttl", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens().len(), 1, "fresh activity keeps the session");
        backdate_activity(temp.path(), "pn-ttl", 61);
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-ttl", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens().len(), 2);
        assert_eq!(current_generation(&temp.path().join("pn-ttl")), 2);
        assert!(temp.path().join("pn-ttl/1").is_dir());
        assert!(logged(
            "Session pn-ttl expired after 60 min idle; starting fresh"
        ));
    }
    #[tokio::test]
    async fn absent_or_zero_ttl_never_expires() {
        for idle_minutes in [None, Some(0)] {
            let temp = tempfile::tempdir().unwrap();
            let probe = Probe::default();
            let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
            let cfg = Config {
                sessions: Sessions { idle_minutes },
                ..Default::default()
            };
            let transport = FakeTransport::default();
            let mut chat = new_chat("pn-1");
            run(
                &mut chat,
                &ctx,
                &cfg,
                temp.path(),
                turn_in("pn-1", "hello"),
                &transport,
            )
            .await;
            backdate_activity(temp.path(), "pn-1", 100_000);
            run(
                &mut chat,
                &ctx,
                &cfg,
                temp.path(),
                turn_in("pn-1", "hello"),
                &transport,
            )
            .await;
            assert_eq!(probe.opens().len(), 1);
            assert_eq!(current_generation(&temp.path().join("pn-1")), 1);
        }
    }
    #[tokio::test]
    async fn failed_turn_keeps_session_and_replies() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-fail");
        chat.notes.push("pn-fail", "Ana reacted 👍".into());
        let cfg = Config::default();
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-fail", "fail"),
            &transport,
        )
        .await;
        assert!(chat.connection.is_some());
        assert!(chat.notes.render("pn-fail").is_some(), "kept for retry");
        assert!(logged("turn failed for pn-fail: failed"));
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-fail", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens().len(), 1);
        assert!(
            chat.notes.render("pn-fail").is_none(),
            "cleared after success"
        );
        assert!(transport
            .0
            .lock()
            .unwrap()
            .contains(&"send:(turn failed)".into()));
    }
    #[tokio::test]
    async fn session_closed_drops_and_reopens_with_resume() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-close");
        let cfg = Config::default();
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-close", "close"),
            &transport,
        )
        .await;
        assert!(chat.connection.is_none());
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-close", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens().len(), 2);
        assert!(chat.connection.is_some());
    }
    #[tokio::test]
    async fn new_command_rotates_clears_notes_and_replies() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-new");
        let cfg = Config::default();
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-new", "hello"),
            &transport,
        )
        .await;
        chat.notes.push("pn-new", "later".into());
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            command("pn-new", "3361", Command::New),
            &transport,
        )
        .await;
        assert!(chat.connection.is_none());
        assert!(chat.notes.render("pn-new").is_none());
        assert_eq!(current_generation(&temp.path().join("pn-new")), 2);
        assert_eq!(
            sends(&transport).last().unwrap(),
            "send:New session started."
        );
        assert_eq!(probe.prompts.lock().unwrap().len(), 1, "no agent turn");
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-new", "hello"),
            &transport,
        )
        .await;
        assert_eq!(probe.opens().len(), 2);
    }
    #[tokio::test]
    async fn compact_command_replies_without_relaying_backend_text() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-c");
        run(
            &mut chat,
            &ctx,
            &Config::default(),
            temp.path(),
            command("pn-c", "3361", Command::Compact),
            &transport,
        )
        .await;
        assert_eq!(probe.prompts.lock().unwrap().as_slice(), ["/compact"]);
        assert_eq!(sends(&transport), vec!["send:Compacted."]);
    }
    #[tokio::test]
    async fn custom_messages_are_used() {
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "messages": { "new_session": "Nouvelle session.", "stopped": "Arrêté." },
            "sessions": { "idle_minutes": 30 }
        }))
        .unwrap();
        assert_eq!(cfg.sessions.idle_minutes, Some(30));
        assert_eq!(cfg.messages.compacted, "Compacted.");
        let defaults = Config::default();
        assert_eq!(defaults.sessions.idle_minutes, None);
        assert_eq!(defaults.messages.stopped, "Stopped.");
        assert_eq!(defaults.messages.new_session, "New session started.");
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-m");
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            command("pn-m", "3361", Command::New),
            &transport,
        )
        .await;
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            command("pn-m", "3361", Command::Stop),
            &transport,
        )
        .await;
        assert_eq!(
            sends(&transport),
            vec!["send:Nouvelle session.", "send:Arrêté."]
        );
    }
    #[test]
    fn commands_need_exact_text_and_group_mention() {
        let bot = vec!["111".to_owned()];
        assert_eq!(parse_command(" /new ", false, &bot), Some(Command::New));
        assert_eq!(parse_command("/stop", false, &bot), Some(Command::Stop));
        assert_eq!(
            parse_command("/compact", false, &bot),
            Some(Command::Compact)
        );
        assert_eq!(parse_command("/new now", false, &bot), None);
        assert_eq!(parse_command("/New", false, &bot), None);
        assert_eq!(parse_command("@111 /new", true, &bot), Some(Command::New));
        assert_eq!(parse_command("/stop @111", true, &bot), Some(Command::Stop));
        assert_eq!(parse_command("/new", true, &bot), None);
        assert_eq!(parse_command("@222 /new", true, &bot), None);
        assert_eq!(parse_command("@111 /new please", true, &bot), None);
    }
    #[tokio::test]
    async fn stop_aborts_in_flight_turn_and_drops_partial_reply() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut workers = Workers::new(ctx, Config::default(), temp.path().to_owned());
        workers.submit(turn_in("pn-s", "slow"), transport.clone());
        wait_for("turn start", || probe.prompts.lock().unwrap().len() == 1).await;
        workers.submit(command("pn-s", "3361", Command::Stop), transport.clone());
        wait_for("stop reply", || sends(&transport) == ["send:Stopped."]).await;
        wait_for("turn end", || {
            transport
                .0
                .lock()
                .unwrap()
                .contains(&"typing:false".to_owned())
        })
        .await;
        assert_eq!(sends(&transport), vec!["send:Stopped."]);
        workers.submit(turn_in("pn-s", "hello"), transport.clone());
        wait_for("next turn", || sent_hello(&transport.0.lock().unwrap())).await;
        assert_eq!(probe.opens().len(), 1, "stop keeps the session");
    }
    #[tokio::test]
    async fn stop_when_idle_only_replies() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-i");
        run(
            &mut chat,
            &ctx,
            &Config::default(),
            temp.path(),
            command("pn-i", "3361", Command::Stop),
            &transport,
        )
        .await;
        assert_eq!(sends(&transport), vec!["send:Stopped."]);
    }
    #[tokio::test]
    async fn auto_compaction_fires_once_at_eighty_percent() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-auto");
        let cfg = Config::default();
        let turn = |text: &'static str| turn_in("pn-auto", text);
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn("small"),
            &transport,
        )
        .await;
        assert_eq!(probe.compactions(), 0);
        run(&mut chat, &ctx, &cfg, temp.path(), turn("big"), &transport).await;
        assert_eq!(probe.compactions(), 1);
        assert!(logged("Auto-compacting pn-auto (85% of context)"));
        assert!(logged("Compacted pn-auto"));
        assert_eq!(sends(&transport).len(), 2, "compaction sends nothing");
        run(&mut chat, &ctx, &cfg, temp.path(), turn("big"), &transport).await;
        assert_eq!(probe.compactions(), 1, "no re-trigger while still over");
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn("small"),
            &transport,
        )
        .await;
        run(&mut chat, &ctx, &cfg, temp.path(), turn("big"), &transport).await;
        assert_eq!(probe.compactions(), 2);
    }
    #[test]
    fn compact_threshold_handles_huge_windows() {
        assert!(!over_compact_threshold(u64::MAX / 2, u64::MAX));
        assert!(over_compact_threshold(u64::MAX, u64::MAX));
    }
    #[tokio::test]
    async fn auto_compaction_threshold_is_inclusive_at_eighty_percent() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-edge");
        let cfg = Config::default();
        let turn = |text: &'static str| turn_in("pn-edge", text);
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn("under"),
            &transport,
        )
        .await;
        assert_eq!(probe.compactions(), 0, "79% stays below the threshold");
        run(&mut chat, &ctx, &cfg, temp.path(), turn("edge"), &transport).await;
        assert_eq!(probe.compactions(), 1, "80% triggers compaction");
    }
    #[tokio::test]
    async fn usage_without_window_clears_stale_usage() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-stale");
        let cfg = Config::default();
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-stale", "stale"),
            &transport,
        )
        .await;
        assert_eq!(probe.compactions(), 0, "a windowless report replaces 85%");
    }
    #[tokio::test]
    async fn finish_without_shutdown_closes_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let probe = Probe::default();
        let (ctx, _shutdown) = context_with(temp.path(), probe.clone());
        let transport = FakeTransport::default();
        let mut workers = Workers::new(ctx, Config::default(), temp.path().to_owned());
        workers.submit(turn_in("pn-f", "hang"), transport.clone());
        wait_for("turn start", || probe.prompts.lock().unwrap().len() == 1).await;
        workers.finish(Duration::from_secs(1)).await;
        assert_eq!(*probe.closes.lock().unwrap(), 1);
    }
    #[tokio::test]
    async fn compaction_records_activity_after_finishing() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut chat = new_chat("pn-ca");
        let cfg = Config::default();
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            turn_in("pn-ca", "hello"),
            &transport,
        )
        .await;
        backdate_activity(temp.path(), "pn-ca", 100);
        assert!(chat.compact(&cfg, temp.path(), &transport, false).await);
        let last = read_number(&temp.path().join("pn-ca").join(ACTIVITY_FILE)).unwrap();
        assert!(now_secs() - last < 5);
    }
    #[tokio::test]
    async fn compact_open_failure_is_logged() {
        dar_extension_sdk::log::set_event_hook(capture);
        let temp = tempfile::tempdir().unwrap();
        let (ctx, _shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let cfg = Config {
            backend: Some("missing".into()),
            ..Default::default()
        };
        let mut chat = new_chat("pn-of");
        run(
            &mut chat,
            &ctx,
            &cfg,
            temp.path(),
            command("pn-of", "3361", Command::Compact),
            &transport,
        )
        .await;
        assert!(EVENTS.lock().unwrap().iter().any(|e| e.starts_with(
            "whatsapp-web: Compaction failed for pn-of: chat backend 'missing' not registered"
        )));
        assert!(sends(&transport).is_empty());
    }
    #[tokio::test]
    async fn chats_run_concurrently() {
        let temp = tempfile::tempdir().unwrap();
        let (ctx, shutdown) = context(temp.path());
        let transport = FakeTransport::default();
        let mut workers = Workers::new(ctx, Config::default(), temp.path().to_owned());
        workers.submit(turn_in("pn-a", "hang"), transport.clone());
        workers.submit(turn_in("pn-b", "hello"), transport.clone());
        wait_for("second chat reply", || {
            sent_hello(&transport.0.lock().unwrap())
        })
        .await;
        shutdown.send(true).unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            workers.finish(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    }
    #[test]
    fn phone_validation() {
        assert!(digits("3361"));
        assert!(!digits("+3361"));
    }
}
