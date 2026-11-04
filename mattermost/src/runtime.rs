use crate::{
    addressing::{self, BotIdentity, InboundPost, RouteDecision},
    api::{MattermostClient, Post, User},
    artifacts, attachments, commands,
    config::MattermostConfig,
    history::{History, HistoryPost},
    live_answer::{self, LiveAnswer},
    session::{self, SessionKey},
    thinking::Thinking,
    ws,
};
use anyhow::{Context, Result};
use dar_extension_sdk::{
    chat::{AgentSender, ChatBackend, ChatEvent, ChatRole},
    StartCtx,
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct ActiveTurn {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}
struct AcceptedTurn {
    post: Post,
    user: User,
    key: SessionKey,
    root: Option<String>,
    text: String,
    expired: bool,
}
impl ActiveTurn {
    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}
struct Runtime {
    ctx: StartCtx,
    cfg: MattermostConfig,
    client: MattermostClient,
    data: PathBuf,
    bot: BotIdentity,
    cancel: CancellationToken,
    turns: Mutex<HashMap<SessionKey, ActiveTurn>>,
    history: History,
    guards: std::sync::Mutex<HashMap<SessionKey, cap_chat::LoopGuard>>,
    latest: std::sync::Mutex<HashMap<SessionKey, i64>>,
}

pub async fn run(
    ctx: StartCtx,
    cfg: MattermostConfig,
    client: MattermostClient,
    data: PathBuf,
) -> Result<()> {
    let mut shutdown = ctx.shutdown.clone();
    let me = tokio::select! {_=shutdown.cancelled()=>return Ok(()),result=client.me()=>result?};
    let cancel = CancellationToken::new();
    let runtime = Arc::new(Runtime {
        ctx,
        cfg,
        client: client.clone(),
        data,
        bot: BotIdentity {
            id: me.id,
            username: me.username,
        },
        cancel: cancel.clone(),
        turns: Mutex::new(HashMap::new()),
        history: History::default(),
        guards: Default::default(),
        latest: Default::default(),
    });
    let (tx, mut rx) = mpsc::unbounded_channel();
    let socket = tokio::spawn(ws::run(client, tx.clone(), cancel.clone()));
    let mut lanes: HashMap<String, mpsc::UnboundedSender<(Post, String, Vec<String>)>> =
        HashMap::new();
    let mut workers = tokio::task::JoinSet::new();
    let mut recent = ws::RecentPosts::new(4096);
    dar_extension_sdk::log::event("-", "mattermost", "connection runtime started");
    loop {
        tokio::select! {
            _=shutdown.cancelled()=>break,
            event=rx.recv()=>match event {
                Some(ws::Event::Posted{post,channel_type,mentions})=>{
                    if !recent.insert(&post.id){continue;}
                    let lane=lanes.entry(post.channel_id.clone()).or_insert_with(||{
                        let (tx,mut rx)=mpsc::unbounded_channel();let runtime=runtime.clone();
                        workers.spawn(async move{while let Some((post,kind,mentions))=rx.recv().await {if runtime.cancel.is_cancelled(){break;}runtime.clone().process(post,kind,mentions).await;}});
                        tx
                    });
                    let _=lane.send((*post,channel_type,mentions));
                }
                Some(ws::Event::Gap{since})=>{
                    let runtime=runtime.clone();let tx=tx.clone();
                    let watched=lanes.keys().cloned().collect::<HashSet<_>>();
                    workers.spawn(async move{tokio::select!{_=runtime.cancel.cancelled()=>{},_=runtime.catch_up(watched,since,tx)=>{}}});
                },
                None=>break,
            }
        }
    }
    cancel.cancel();
    drop(lanes);
    let _ = socket.await;
    while workers.join_next().await.is_some() {}
    let turns = std::mem::take(&mut *runtime.turns.lock().await);
    for turn in turns.values() {
        turn.cancel.cancel();
    }
    for (_, turn) in turns {
        let _ = turn.task.await;
    }
    Ok(())
}

impl Runtime {
    async fn catch_up(
        &self,
        mut watched: HashSet<String>,
        since: i64,
        tx: mpsc::UnboundedSender<ws::Event>,
    ) {
        if let Some(channels) = &self.cfg.channels {
            watched.extend(channels.keys().cloned());
        } else {
            match self.client.list_channels(usize::MAX, None).await {
                Ok(channels) => watched.extend(channels.into_iter().map(|c| c.id)),
                Err(_) => tracing::warn!("mattermost recovery channel listing failed"),
            }
        }
        let mut recovered = Vec::new();
        for channel in watched {
            let result = async {
                let kind = self.client.channel(&channel).await?.kind;
                let page = self
                    .client
                    .channel_posts(&channel, None, Some(since), 200)
                    .await?;
                Ok::<_, crate::api::ApiError>((kind, page))
            }
            .await;
            match result {
                Ok((kind, page)) => {
                    for post in page.posts.into_values().filter(|p| p.create_at >= since) {
                        recovered.push((post, kind.clone()));
                    }
                }
                Err(_) => tracing::warn!("mattermost catch-up failed for a watched channel"),
            }
        }
        recovered.sort_by_key(|(post, _)| post.create_at);
        for (post, kind) in recovered {
            let _ = tx.send(ws::Event::Posted {
                post: Box::new(post),
                channel_type: kind,
                mentions: Vec::new(),
            });
        }
    }
    async fn process(self: Arc<Self>, post: Post, mut kind: String, mentions: Vec<String>) {
        if post.user_id == self.bot.id
            || flag(&post, "from_webhook")
            || flag(&post, "from_plugin")
            || !post.post_type.is_empty()
        {
            return;
        }
        if kind.is_empty() {
            match self.client.channel(&post.channel_id).await {
                Ok(channel) => kind = channel.kind,
                Err(_) => {
                    return;
                }
            }
        }
        if (kind == "D" && !self.cfg.dm.enabled)
            || (kind != "D"
                && self
                    .cfg
                    .channels
                    .as_ref()
                    .is_some_and(|channels| !channels.contains_key(&post.channel_id)))
        {
            return;
        }
        let identity = self.client.user(&post.user_id).await;
        let identity_failed = identity.is_err();
        let user = identity.unwrap_or_else(|_| User {
            id: post.user_id.clone(),
            ..Default::default()
        });
        let engaged = !post.root_id.is_empty()
            && session::is_active_engagement(
                &self.data,
                &SessionKey::thread(kind == "D", &post.channel_id, &post.root_id),
                self.cfg.sessions.idle_minutes,
                session::now(),
            );
        let inbound = InboundPost {
            id: &post.id,
            channel_id: &post.channel_id,
            channel_type: &kind,
            root_id: &post.root_id,
            author_id: &post.user_id,
            author_username: &user.username,
            text: &post.message,
            post_type: &post.post_type,
            from_bot: flag(&post, "from_bot") || user.is_bot,
            from_webhook: false,
            from_plugin: false,
            has_files: !post.file_ids.is_empty(),
            thread_engaged: engaged,
            mentions: &mentions,
        };
        let route = addressing::route(&self.cfg, &self.bot, &inbound);
        let history_key = match &route {
            RouteDecision::Dispatch { conversation, .. } => conversation.clone(),
            _ if !post.root_id.is_empty() => {
                SessionKey::thread(kind == "D", &post.channel_id, &post.root_id)
            }
            _ if kind == "D" => SessionKey::dm(&post.channel_id),
            _ => SessionKey::channel(&post.channel_id),
        };
        if !identity_failed {
            self.record(&history_key, &post, &user);
        }
        let RouteDecision::Dispatch {
            text,
            conversation,
            reply_root,
        } = route
        else {
            return;
        };
        if identity_failed {
            self.failure(&post, reply_root.as_deref()).await;
            return;
        }
        if post.create_at > 0 {
            let mut latest = self.latest.lock().expect("post ordering lock poisoned");
            if latest
                .get(&conversation)
                .is_some_and(|at| post.create_at < *at)
            {
                return;
            }
            latest.insert(conversation.clone(), post.create_at);
        }
        dar_extension_sdk::log::event("-", "mattermost", "accepted a post");
        let previous = { self.turns.lock().await.remove(&conversation) };
        if let Some(previous) = previous {
            previous.stop().await;
        }
        if self.cfg.ack_reaction
            && self
                .client
                .add_reaction(&self.bot.id, &post.id, &self.cfg.ack_emoji)
                .await
                .is_err()
        {
            tracing::warn!("mattermost acknowledgement failed");
        }
        if let Some(command) = commands::parse(&text) {
            if command == commands::Command::Reset {
                if session::reset_with_activity(&self.data, &conversation, session::now()).is_err()
                {
                    self.failure(&post, reply_root.as_deref()).await;
                    return;
                }
                self.history.clear(conversation.as_str());
            }
            if self
                .client
                .create_post(
                    &post.channel_id,
                    commands::reply(command),
                    reply_root.as_deref(),
                    &[],
                )
                .await
                .is_err()
            {
                self.failure(&post, reply_root.as_deref()).await;
            }
            return;
        }
        let prepared = (|| -> Result<bool> {
            let expired = session::prepare_activity(
                &self.data,
                &conversation,
                self.cfg.sessions.idle_minutes,
                session::now(),
            )?;
            if expired {
                self.history.clear(conversation.as_str());
                self.record(&conversation, &post, &user);
            }
            if conversation.is_thread() {
                session::engage(&self.data, &conversation)?;
            }
            Ok(expired)
        })();
        let expired = match prepared {
            Ok(expired) => expired,
            Err(_) => {
                self.failure(&post, reply_root.as_deref()).await;
                return;
            }
        };
        let cancel = self.cancel.child_token();
        let runtime = self.clone();
        let turn_cancel = cancel.clone();
        let turn = AcceptedTurn {
            post,
            user,
            key: conversation.clone(),
            root: reply_root,
            text,
            expired,
        };
        let task = tokio::spawn(async move {
            let typing = async {
                loop {
                    let _ = runtime
                        .client
                        .typing(&runtime.bot.id, &turn.post.channel_id, turn.root.as_deref())
                        .await;
                    tokio::time::sleep(Duration::from_secs(4)).await;
                }
            };
            let result = tokio::select! {result=runtime.answer(&turn,&turn_cancel)=>result,()=typing=>unreachable!()};
            match result {
                Ok(true) if runtime.cfg.ack_reaction => {
                    let _ = runtime
                        .client
                        .remove_reaction(&runtime.bot.id, &turn.post.id, &runtime.cfg.ack_emoji)
                        .await;
                }
                Err(_) if !turn_cancel.is_cancelled() => {
                    runtime.failure(&turn.post, turn.root.as_deref()).await
                }
                _ => {}
            }
        });
        self.turns
            .lock()
            .await
            .insert(conversation, ActiveTurn { cancel, task });
    }
    async fn failure(&self, post: &Post, root: Option<&str>) {
        live_answer::failure(&self.client, &self.bot.id, &post.id, &post.channel_id, root).await;
    }
    fn record(&self, key: &SessionKey, post: &Post, user: &User) {
        self.history.record(
            key.as_str(),
            &self.bot.id,
            &HistoryPost {
                id: &post.id,
                author_id: &post.user_id,
                display_name: &user.name(),
                username: &user.username,
                text: &post.message,
                is_bot: flag(post, "from_bot") || user.is_bot,
                from_webhook: flag(post, "from_webhook"),
            },
        );
    }
    async fn seed_history(&self, post: &Post, key: &SessionKey) -> Result<()> {
        let limit = if self.cfg.history_limit == 0 {
            50
        } else {
            self.cfg.history_limit.min(50)
        };
        let page = if let Some(root) = key.root_id() {
            self.client.thread(root).await?
        } else {
            self.client
                .channel_posts(&post.channel_id, Some(&post.id), None, limit)
                .await?
        };
        let mut earlier = page
            .order
            .iter()
            .filter_map(|id| page.posts.get(id))
            .filter(|p| {
                p.id != post.id
                    && p.user_id != self.bot.id
                    && !flag(p, "from_webhook")
                    && !flag(p, "from_plugin")
                    && p.post_type.is_empty()
                    && (post.create_at == 0 || p.create_at <= post.create_at)
                    && (key.is_thread() || p.root_id.is_empty())
            })
            .take(limit)
            .collect::<Vec<_>>();
        earlier.reverse();
        let ids = earlier
            .iter()
            .map(|p| p.user_id.clone())
            .collect::<Vec<_>>();
        let users = self
            .client
            .users_by_ids(&ids)
            .await?
            .into_iter()
            .map(|u| (u.id.clone(), u))
            .collect::<HashMap<_, _>>();
        let older = earlier
            .into_iter()
            .filter_map(|p| {
                let user = users.get(&p.user_id)?;
                let label = if flag(p, "from_bot") || user.is_bot {
                    format!("{} (@{}, bot)", user.name(), user.username)
                } else {
                    format!("{} (@{})", user.name(), user.username)
                };
                let text = if p.message.is_empty() && !p.file_ids.is_empty() {
                    "[attachment]"
                } else {
                    &p.message
                };
                Some((p.id.clone(), format!("{label}: {text}")))
            })
            .collect();
        self.history.seed(key.as_str(), older);
        Ok(())
    }
    async fn answer(&self, turn: &AcceptedTurn, cancel: &CancellationToken) -> Result<bool> {
        let AcceptedTurn {
            post,
            user,
            key,
            root,
            text,
            expired,
        } = turn;
        let root = root.as_deref();
        let expired = *expired;
        if self.cfg.fetch_history && self.history.claim_seed(key.as_str()) && !expired {
            tokio::select! {_=cancel.cancelled()=>return Ok(false),result=self.seed_history(post,key)=>if result.is_err(){tracing::warn!("mattermost history backfill failed");}}
        }
        let directory = session::prepare(&self.data, key)?;
        let backend_id =
            dar_extension_sdk::chat::resolve_agent_backend(&self.ctx, self.cfg.backend.as_deref());
        let backend = self
            .ctx
            .host
            .services
            .get::<dyn ChatBackend>(&backend_id)
            .context("chat backend unavailable")?;
        let (tx, mut events) = mpsc::channel(256);
        let (artifact_tx, mut artifact_rx) = mpsc::channel(32);
        let params = dar_extension_sdk::chat::agent_session_params(&self.ctx, &directory)
            .resume_session_id(session::resume_id(&directory))
            .artifact_ready(Some(artifact_tx))
            .build();
        let sender = (flag(post, "from_bot") || user.is_bot).then(|| AgentSender {
            agent_id: format!("mattermost:{}", post.user_id),
            hops: None,
        });
        let admitted = self
            .guards
            .lock()
            .expect("loop guard lock poisoned")
            .entry(key.clone())
            .or_insert_with(|| cap_chat::LoopGuard::new(params.agent_loop))
            .admit(sender.as_ref())
            .is_ok();
        if !admitted {
            dar_extension_sdk::log::event("-", "mattermost", "loop guard dropped a turn");
            return Ok(true);
        }
        let current=format!("From {} (@{}):\n{text}\n\nTo mention a user or another bot, write @username; a bare name does not notify them. Bots only respond when mentioned.",user.name(),user.username);
        let current = tokio::select! {_=cancel.cancelled()=>return Ok(false),result=attachments::prompt(&self.client,self.ctx.paths.root(),&post.file_ids,current)=>result?};
        let prompt = self
            .history
            .prompt(key.as_str(), &post.id, &current, self.cfg.history_limit);
        let bot_triggered = sender.is_some();
        let mut chat = tokio::select! {_=cancel.cancelled()=>return Ok(false),result=tokio::time::timeout(Duration::from_secs(60),backend.open(params,tx))=>result.context("agent open timeout")??};
        let mut live = LiveAnswer::new(
            self.client.clone(),
            post.channel_id.clone(),
            root.map(str::to_owned),
        );
        if bot_triggered {
            live = live.without_streaming();
        }
        let mut notice = LiveAnswer::new(
            self.client.clone(),
            post.channel_id.clone(),
            root.map(str::to_owned),
        )
        .without_streaming();
        let mut thinking = self.cfg.show_thinking.then(|| {
            Thinking::new(
                self.client.clone(),
                post.channel_id.clone(),
                root.map(str::to_owned),
                self.cfg.delete_thinking_on_complete,
            )
        });
        let mut silent = false;
        let mut collected = Vec::<dar_extension_sdk::chat::ArtifactReady>::new();
        let mut result=async {
            tokio::select!{_=cancel.cancelled()=>return Ok(false),result=tokio::time::timeout(Duration::from_secs(60),chat.send_turn_from(prompt,sender))=>result.context("agent queue timeout")??};
            let mut reply=String::new();
            let mut deadline=tokio::time::Instant::now()+Duration::from_secs(60);
            let mut artifacts_open=true;
            loop {
                tokio::select! {
                    _=cancel.cancelled()=>return Ok(false),
                    _=tokio::time::sleep_until(deadline)=>anyhow::bail!("agent event timeout"),
                    _=live.wait_for_flush()=>live.flush_if_due(&reply).await?,
                    artifact=artifact_rx.recv(),if artifacts_open=>match artifact{Some(artifact)=>{if !collected.iter().any(|a|a.id==artifact.id){collected.push(artifact);}},None=>artifacts_open=false},
                    _=async{if let Some(thinking)=&thinking{thinking.wait_for_flush().await}else{std::future::pending::<()>().await}}=>if let Some(thinking)=&mut thinking{thinking.flush_if_due().await?;},
                    event=events.recv()=>{deadline=tokio::time::Instant::now()+Duration::from_secs(60);match event {
                        Some(ChatEvent::Delta{role:ChatRole::Assistant,text}) if !silent=>{reply.push_str(&text);if expired{notice.finish(session::EXPIRED_NOTICE).await?;}live.push(&reply).await?;}
                        Some(ChatEvent::Delta{role:ChatRole::Thinking,text}) if !silent=>{if let Some(thinking)=&mut thinking{if expired{notice.finish(session::EXPIRED_NOTICE).await?;}thinking.append(&text).await?;}}
                        Some(ChatEvent::Silent{..})=>{silent=true;live.discard().await;notice.discard().await;if let Some(thinking)=&mut thinking{let _=thinking.finish(true).await;}}
                        Some(ChatEvent::TurnFinished{ok,..})=>{if !ok&&!silent{anyhow::bail!("agent failed");}break;},
                        Some(ChatEvent::SessionClosed{..})|None=>anyhow::bail!("agent closed before completion"),
                        _=>{},
                    }}
                }
            }
            if silent{return Ok(true);}
            if reply.trim().is_empty(){reply="(no response)".into();}
            if expired{notice.finish(session::EXPIRED_NOTICE).await?;}
            live.finish(&reply).await?;
            while let Ok(artifact)=artifact_rx.try_recv(){if !collected.iter().any(|a|a.id==artifact.id){collected.push(artifact);}}
            if !collected.is_empty(){
                let store=self.ctx.host.services.get_named::<dar_extension_sdk::artifacts::ArtifactStore>("artifact-store").context("artifact store unavailable")?;
                artifacts::deliver(&store,&self.client,&collected,&post.channel_id,root).await?;
            }
            if self.cfg.clear_history_after_reply{self.history.clear(key.as_str());}
            Ok(false)
        }.await;
        if silent {
            result = Ok(true);
            live.discard().await;
            notice.discard().await;
        }
        if let Some(thinking) = &mut thinking {
            if let Err(error) = thinking
                .finish(silent || cancel.is_cancelled() || result.is_err())
                .await
            {
                if !silent && !cancel.is_cancelled() {
                    result = Err(error);
                }
            }
        }
        if cancel.is_cancelled() || result.is_err() {
            let _ = tokio::time::timeout(Duration::from_secs(5), chat.abort()).await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), chat.close()).await;
        result
    }
}
fn flag(post: &Post, name: &str) -> bool {
    post.props[name].as_bool().unwrap_or(false) || post.props[name].as_str() == Some("true")
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
