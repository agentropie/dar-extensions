//! Poll loop, per-item flow, and the chat turn driver.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use dar_extension_sdk::chat::{
    is_no_reply, AgentSender, ChatEvent, ChatRole, ChatSession, TurnOrigin,
};
use dar_extension_sdk::ShutdownToken;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::api::{split_answer, Account, Board, BoardError, Message, MAX_BODY_CHARS};
use crate::labels::tool_label;
use crate::prompt::build_prompt;

const WORKING: &str = "👀";
const DONE: &str = "✅";
const STOP_WAIT: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(600);
const FAILURE_REPLY: &str = "I could not finish this request.";
const STOPPED_REPLY: &str = "Stopped on request.";

pub struct ChatConn {
    pub session: Box<dyn ChatSession>,
    pub rx: mpsc::Receiver<ChatEvent>,
}

/// Opens a chat session in a directory. The host implements it; tests use a mock.
#[async_trait]
pub trait SessionOpener: Send + Sync {
    async fn open(&self, dir: &Path) -> Result<ChatConn>;
}

/// How a turn ended.
#[derive(Debug, PartialEq)]
pub enum TurnEnd {
    Answer(String),
    Silent,
    Question(String),
    Stopped,
    Failed(&'static str),
    ThreadClosed,
    Shutdown,
}

pub struct TurnReport {
    pub end: TurnEnd,
    /// The session is not usable any more.
    pub drop_session: bool,
}

enum Posted {
    Ok { stop: bool },
    Closed,
    Failed,
}

async fn post_activity(
    board: &Board,
    item_id: i64,
    kind: &str,
    body: &str,
    ephemeral: bool,
) -> Posted {
    match board.activity(item_id, kind, body, ephemeral).await {
        Ok(session) => Posted::Ok {
            stop: session.stop_requested.is_some(),
        },
        Err(BoardError::ThreadClosed) => Posted::Closed,
        Err(err) => {
            tracing::warn!(item = item_id, error = %err, "threadit activity failed");
            Posted::Failed
        }
    }
}

/// Abort the turn and wait for the `TurnFinished` of the submitted run.
/// The `wait` limit covers the `abort()` call and the drain. True when the session is still usable.
/// `origin` is the origin of the run the caller already consumed.
async fn abort_and_drain(
    conn: &mut ChatConn,
    wait: Duration,
    mut origin: Option<TurnOrigin>,
) -> bool {
    let work = async {
        if conn.session.abort().await.is_err() {
            return false;
        }
        loop {
            match conn.rx.recv().await {
                Some(ChatEvent::TurnStarted { origin: started }) => origin = Some(started),
                Some(ChatEvent::TurnFinished { .. }) => {
                    if origin.take() != Some(TurnOrigin::Autonomous) {
                        return true;
                    }
                }
                Some(ChatEvent::SessionClosed { .. }) | None => return false,
                Some(_) => {}
            }
        }
    };
    tokio::time::timeout(wait, work).await.unwrap_or(false)
}

async fn end_for_posted(
    posted: Posted,
    conn: &mut ChatConn,
    stop_wait: Duration,
    origin: Option<TurnOrigin>,
) -> Option<TurnReport> {
    let end = match posted {
        Posted::Closed => TurnEnd::ThreadClosed,
        Posted::Ok { stop: true } => TurnEnd::Stopped,
        _ => return None,
    };
    let usable = abort_and_drain(conn, stop_wait, origin).await;
    Some(TurnReport {
        end,
        drop_session: !usable,
    })
}

fn question_text(questions: &[dar_extension_sdk::chat::QuestionInfo]) -> String {
    let mut out = String::new();
    for q in questions {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(q.question.trim());
        for option in &q.options {
            out.push_str(&format!("\n- {}", option.label));
            if !option.description.is_empty() {
                out.push_str(&format!(": {}", option.description));
            }
        }
    }
    if out.is_empty() {
        out.push_str("The agent needs more input.");
    }
    out
}

/// Settings for one turn.
pub struct TurnParams {
    pub item_id: i64,
    pub me_id: i64,
    pub poll: Duration,
    pub heartbeat: Duration,
    /// Longest time the turn may run.
    pub timeout: Duration,
    /// Longest time an abort and drain may take.
    pub stop_wait: Duration,
}

/// Send the prompt and map chat events to board activities until the turn ends.
pub async fn drive_turn(
    board: &Board,
    conn: &mut ChatConn,
    params: &TurnParams,
    prompt: String,
    sender: Option<AgentSender>,
    shutdown: &mut ShutdownToken,
) -> TurnReport {
    let deadline = tokio::time::sleep(params.timeout);
    tokio::pin!(deadline);
    let send = async {
        match sender {
            Some(_) => conn.session.send_turn_from(prompt, sender).await,
            None => conn.session.send_turn(prompt).await,
        }
    };
    let sent = tokio::select! {
        _ = shutdown.cancelled() => {
            return TurnReport { end: TurnEnd::Shutdown, drop_session: true };
        }
        _ = &mut deadline => {
            return TurnReport { end: TurnEnd::Failed("timed out"), drop_session: true };
        }
        sent = send => sent,
    };
    if sent.is_err() {
        return TurnReport {
            end: TurnEnd::Failed("backend closed"),
            drop_session: true,
        };
    }

    let item_id = params.item_id;
    let mut answer = String::new();
    let mut silent = false;
    let mut origin: Option<TurnOrigin> = None;
    let mut last_label: Option<String> = None;
    let mut last_post = Instant::now();
    let mut ticker = tokio::time::interval(params.poll);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        enum Step {
            Shutdown,
            Timeout,
            Tick,
            Event(Option<ChatEvent>),
        }
        let step = tokio::select! {
            _ = shutdown.cancelled() => Step::Shutdown,
            _ = &mut deadline => Step::Timeout,
            _ = ticker.tick() => Step::Tick,
            event = conn.rx.recv() => Step::Event(event),
        };
        match step {
            Step::Shutdown => {
                let _ = abort_and_drain(conn, params.stop_wait, origin).await;
                return TurnReport {
                    end: TurnEnd::Shutdown,
                    drop_session: true,
                };
            }
            Step::Timeout => {
                let _ = abort_and_drain(conn, params.stop_wait, origin).await;
                return TurnReport {
                    end: TurnEnd::Failed("timed out"),
                    drop_session: true,
                };
            }
            Step::Tick => {
                if let Ok(message) = board.message(item_id).await {
                    if message.stop_requested_for(params.me_id) {
                        let posted = Posted::Ok { stop: true };
                        if let Some(report) =
                            end_for_posted(posted, conn, params.stop_wait, origin).await
                        {
                            return report;
                        }
                    }
                }
                if last_post.elapsed() >= params.heartbeat {
                    let posted =
                        post_activity(board, item_id, "thought", "Still working", true).await;
                    last_post = Instant::now();
                    if let Some(report) =
                        end_for_posted(posted, conn, params.stop_wait, origin).await
                    {
                        return report;
                    }
                }
            }
            Step::Event(None) => {
                return TurnReport {
                    end: TurnEnd::Failed("backend closed"),
                    drop_session: true,
                }
            }
            Step::Event(Some(event)) => match event {
                ChatEvent::TurnStarted { origin: started } => origin = Some(started),
                ChatEvent::Delta {
                    role: ChatRole::Assistant,
                    text,
                } => {
                    if origin != Some(TurnOrigin::Autonomous) {
                        answer.push_str(&text);
                    }
                }
                ChatEvent::Silent { .. } => {
                    if origin != Some(TurnOrigin::Autonomous) {
                        silent = true;
                    }
                }
                ChatEvent::ToolCall { .. } | ChatEvent::QuestionAsked { .. }
                    if origin == Some(TurnOrigin::Autonomous) => {}
                ChatEvent::ToolCall { name, .. } => {
                    let Some(label) = tool_label(&name) else {
                        continue;
                    };
                    if last_label.as_deref() == Some(label.as_str()) {
                        continue;
                    }
                    let posted = post_activity(board, item_id, "action", &label, true).await;
                    last_label = Some(label);
                    last_post = Instant::now();
                    if let Some(report) =
                        end_for_posted(posted, conn, params.stop_wait, origin).await
                    {
                        return report;
                    }
                }
                ChatEvent::QuestionAsked { questions, .. } => {
                    let text = question_text(&questions);
                    let posted = post_activity(board, item_id, "elicitation", &text, false).await;
                    if let Some(report) =
                        end_for_posted(posted, conn, params.stop_wait, origin).await
                    {
                        return report;
                    }
                    let usable = abort_and_drain(conn, params.stop_wait, origin).await;
                    return TurnReport {
                        end: TurnEnd::Question(text),
                        drop_session: !usable,
                    };
                }
                ChatEvent::Error(_) => {
                    tracing::warn!(item = item_id, "threadit backend reported an error");
                }
                ChatEvent::TurnFinished { .. } if origin.take() == Some(TurnOrigin::Autonomous) => {
                }
                ChatEvent::TurnFinished { ok: true, .. } => {
                    let text = std::mem::take(&mut answer);
                    let end = if silent || is_no_reply(&text) {
                        TurnEnd::Silent
                    } else {
                        TurnEnd::Answer(text)
                    };
                    return TurnReport {
                        end,
                        drop_session: false,
                    };
                }
                ChatEvent::TurnFinished { ok: false, error } => {
                    let reason = if error.as_deref() == Some("aborted") {
                        "aborted"
                    } else {
                        "backend failed"
                    };
                    return TurnReport {
                        end: TurnEnd::Failed(reason),
                        drop_session: false,
                    };
                }
                ChatEvent::SessionClosed { .. } => {
                    return TurnReport {
                        end: TurnEnd::Failed("backend closed"),
                        drop_session: true,
                    }
                }
                _ => {}
            },
        }
    }
}

enum Replied {
    Ok,
    Closed,
    /// The request failed before it reached the server.
    Failed,
    /// The request may have reached the server. Never send it again.
    Uncertain,
}

/// State the helpers read. It is `Sync`, so helper futures stay `Send`.
struct Shared {
    board: Board,
    me: Account,
    marker_dir: PathBuf,
}

impl Shared {
    fn marker_path(&self, item_id: i64) -> PathBuf {
        self.marker_dir.join(item_id.to_string())
    }

    fn has_marker(&self, item_id: i64) -> bool {
        self.marker_path(item_id).exists()
    }

    fn clear_marker(&self, item_id: i64) {
        let _ = std::fs::remove_file(self.marker_path(item_id));
    }

    /// Write the delivery marker before the first reply POST.
    fn write_marker(&self, item_id: i64) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.marker_dir)?;
        std::fs::write(self.marker_path(item_id), b"")
    }

    async fn reply_all(&self, item: &Message, text: &str) -> Replied {
        if self.write_marker(item.id).is_err() {
            tracing::warn!(
                item = item.id,
                "threadit could not write the delivery marker"
            );
            return Replied::Failed;
        }
        for part in split_answer(text, MAX_BODY_CHARS) {
            match self.board.reply(item.thread_id, item.id, &part).await {
                Ok(()) => {}
                Err(BoardError::ThreadClosed) => {
                    self.clear_marker(item.id);
                    return Replied::Closed;
                }
                Err(err) => {
                    tracing::warn!(item = item.id, error = %err, "threadit reply failed");
                    if !err.is_uncertain() {
                        self.clear_marker(item.id);
                    }
                    return if err.is_uncertain() {
                        Replied::Uncertain
                    } else {
                        Replied::Failed
                    };
                }
            }
        }
        Replied::Ok
    }

    async fn mark_done(&self, item: &Message) {
        match self.board.react(item.id, DONE).await {
            Ok(()) => self.clear_marker(item.id),
            Err(err) => {
                tracing::warn!(item = item.id, error = %err, "threadit could not add the done reaction");
            }
        }
    }

    async fn fail_item(&self, item: &Message, reason: &str) {
        let body = format!("Run failed: {reason}");
        if matches!(
            post_activity(&self.board, item.id, "error", &body, false).await,
            Posted::Closed
        ) {
            return;
        }
        let _ = self.reply_all(item, FAILURE_REPLY).await;
    }

    /// Reply that the run stopped and add ✅. A reply that exists already is not repeated.
    async fn finish_stopped(&self, item: &Message) {
        let replied = item
            .session_of(self.me.id)
            .is_some_and(|s| s.reply_id.is_some());
        if replied {
            self.mark_done(item).await;
        } else {
            self.reply_and_finish(item, STOPPED_REPLY).await;
        }
    }

    /// Reply, then add ✅ when the reply is known to exist.
    /// After an uncertain send, read the message back. Never send again.
    async fn reply_and_finish(&self, item: &Message, text: &str) {
        match self.reply_all(item, text).await {
            Replied::Ok => self.mark_done(item).await,
            Replied::Uncertain => match self.board.message(item.id).await {
                Ok(message)
                    if message
                        .session_of(self.me.id)
                        .is_some_and(|s| s.reply_id.is_some()) =>
                {
                    self.mark_done(item).await
                }
                _ => tracing::warn!(
                    item = item.id,
                    "threadit could not confirm the reply, item stays at working"
                ),
            },
            Replied::Closed | Replied::Failed => {}
        }
    }
}

pub struct Runtime {
    shared: Shared,
    opener: Box<dyn SessionOpener>,
    session_root: PathBuf,
    poll: Duration,
    heartbeat: Duration,
    turn_timeout: Duration,
    stop_wait: Duration,
    sessions: HashMap<String, ChatConn>,
    handled: HashSet<i64>,
    shutdown: ShutdownToken,
}

impl Runtime {
    pub fn new(
        board: Board,
        opener: Box<dyn SessionOpener>,
        me: Account,
        session_root: PathBuf,
        poll: Duration,
        turn_timeout: Duration,
        shutdown: ShutdownToken,
    ) -> Self {
        Self {
            shared: Shared {
                marker_dir: session_root
                    .parent()
                    .unwrap_or(&session_root)
                    .join("delivering"),
                board,
                me,
            },
            opener,
            session_root,
            poll,
            heartbeat: HEARTBEAT,
            turn_timeout,
            stop_wait: STOP_WAIT,
            sessions: HashMap::new(),
            handled: HashSet::new(),
            shutdown,
        }
    }

    #[cfg(test)]
    pub fn with_heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    #[cfg(test)]
    pub fn with_turn_timeout(mut self, timeout: Duration) -> Self {
        self.turn_timeout = timeout;
        self
    }

    #[cfg(test)]
    pub fn with_stop_wait(mut self, stop_wait: Duration) -> Self {
        self.stop_wait = stop_wait;
        self
    }

    pub async fn close_all(&mut self) {
        for (_, conn) in self.sessions.drain() {
            let _ = conn.session.close().await;
        }
    }

    /// Process items left at 👀 by an earlier run, once.
    pub async fn resume_processing(&mut self) {
        let mut items = Vec::new();
        let mut offset = 0;
        loop {
            match self.shared.board.attention("processing", offset).await {
                Ok((page, more)) => {
                    offset += page.len();
                    items.extend(page);
                    if !more {
                        break;
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "threadit could not list processing items");
                    break;
                }
            }
        }
        for item in items {
            if self.shutdown.is_cancelled() {
                return;
            }
            if self.handled.contains(&item.id) {
                continue;
            }
            let state = item
                .session_of(self.shared.me.id)
                .map(|s| s.state.as_str())
                .unwrap_or("");
            match state {
                "working" | "stale" => self.handle_item(item, true).await,
                "stopping" => {
                    self.handled.insert(item.id);
                    self.shared.finish_stopped(&item).await;
                }
                _ => {}
            }
        }
    }

    /// Process the next page of unacknowledged items.
    pub async fn poll_once(&mut self) {
        let items = match self.shared.board.attention("unacknowledged", 0).await {
            Ok((items, _)) => items,
            Err(err) => {
                tracing::warn!(error = %err, "threadit poll failed");
                return;
            }
        };
        for item in items {
            if self.shutdown.is_cancelled() {
                return;
            }
            if !self.handled.contains(&item.id) {
                self.handle_item(item, false).await;
            }
        }
    }

    pub async fn handle_item(&mut self, item: Message, resumed: bool) {
        if item.stop_requested_for(self.shared.me.id) {
            self.handled.insert(item.id);
            self.shared.mark_done(&item).await;
            return;
        }
        // A reply exists already: the earlier run finished. Add ✅ and do not run again.
        let replied = item
            .session_of(self.shared.me.id)
            .is_some_and(|s| s.reply_id.is_some());
        if resumed && replied {
            self.handled.insert(item.id);
            self.shared.mark_done(&item).await;
            return;
        }
        // An earlier run began to deliver a reply. Never run or reply again.
        if resumed && self.shared.has_marker(item.id) {
            self.handled.insert(item.id);
            tracing::warn!(
                item = item.id,
                "threadit reply delivery is unconfirmed, item stays at working"
            );
            return;
        }
        match self.shared.board.react(item.id, WORKING).await {
            Ok(()) => {}
            Err(BoardError::ThreadClosed) => {
                self.handled.insert(item.id);
                return;
            }
            Err(err) => {
                tracing::warn!(item = item.id, error = %err, "threadit could not add the working reaction");
                return;
            }
        }
        self.handled.insert(item.id);
        let first = if resumed {
            "Resumed after restart"
        } else {
            "Started"
        };
        let posted = post_activity(&self.shared.board, item.id, "thought", first, false).await;
        match posted {
            Posted::Closed => return,
            Posted::Ok { stop: true } => {
                self.shared.finish_stopped(&item).await;
                return;
            }
            _ => {}
        }

        let context = match self.shared.board.context(item.id).await {
            Ok(context) => context,
            Err(err) => {
                tracing::warn!(item = item.id, error = %err, "threadit could not read the context");
                self.shared.fail_item(&item, "context unavailable").await;
                return;
            }
        };
        let prompt = build_prompt(self.shared.board.base_url(), &context);

        let key = format!("thread:{}", item.thread_id);
        let conn = match self.sessions.remove(&key) {
            Some(conn) => conn,
            None => {
                let dir = self.session_root.join(format!("thread-{}", item.thread_id));
                match self.opener.open(&dir).await {
                    Ok(conn) => conn,
                    Err(_) => {
                        tracing::warn!(item = item.id, "threadit could not open a chat session");
                        self.shared.fail_item(&item, "backend unavailable").await;
                        return;
                    }
                }
            }
        };
        let mut conn = conn;

        let sender = (item.author.account_type == "agent").then(|| AgentSender {
            agent_id: format!("threadit:{}", item.author.username),
            hops: None,
        });
        let params = TurnParams {
            item_id: item.id,
            me_id: self.shared.me.id,
            poll: self.poll,
            heartbeat: self.heartbeat,
            timeout: self.turn_timeout,
            stop_wait: self.stop_wait,
        };
        let report = drive_turn(
            &self.shared.board,
            &mut conn,
            &params,
            prompt,
            sender,
            &mut self.shutdown,
        )
        .await;
        if report.drop_session {
            let _ = conn.session.close().await;
        } else {
            self.sessions.insert(key, conn);
        }

        match report.end {
            TurnEnd::Answer(text) => {
                let text = if text.trim().is_empty() {
                    "(no response)".to_string()
                } else {
                    text
                };
                self.shared.reply_and_finish(&item, &text).await;
            }
            TurnEnd::Silent => self.shared.mark_done(&item).await,
            TurnEnd::Question(text) => {
                let _ = self.shared.reply_all(&item, &text).await;
            }
            TurnEnd::Stopped => self.shared.reply_and_finish(&item, STOPPED_REPLY).await,
            TurnEnd::Failed(reason) => self.shared.fail_item(&item, reason).await,
            TurnEnd::ThreadClosed => {
                tracing::info!(item = item.id, "threadit thread closed, writes stopped");
            }
            TurnEnd::Shutdown => {}
        }
    }
}

/// Run until shutdown. Returns early when the token is rejected.
pub async fn run(
    board: Board,
    opener: Box<dyn SessionOpener>,
    session_root: PathBuf,
    poll: Duration,
    turn_timeout: Duration,
    mut shutdown: ShutdownToken,
) {
    let me = loop {
        match board.me().await {
            Ok(me) => break me,
            Err(BoardError::Status { status, .. }) if status == 401 || status == 403 => {
                tracing::error!(
                    "threadit rejected THREADIT_TOKEN (HTTP {status}); the account may be pending approval"
                );
                return;
            }
            Err(err) => {
                tracing::warn!(error = %err, "threadit could not reach the board");
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }
    };
    if me.account_type != "agent" {
        tracing::error!("threadit account {} is not an agent account", me.username);
        return;
    }
    tracing::info!(agent = %me.username, "threadit connected");

    let mut rt = Runtime::new(
        board,
        opener,
        me,
        session_root,
        poll,
        turn_timeout,
        shutdown.clone(),
    );
    rt.resume_processing().await;
    while !shutdown.is_cancelled() {
        rt.poll_once().await;
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = tokio::time::sleep(poll) => {}
        }
    }
    rt.close_all().await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use dar_extension_sdk::chat::{BoxFuture, QuestionInfo, QuestionOption};
    use serde_json::Value;
    use tokio::sync::watch;

    use super::*;
    use crate::api::mock::{Handler, MockServer, Req};

    const ITEM: &str = r#"{"id":7,"thread_id":9,"author":{"id":2,"username":"alice","account_type":"human"},"body":"@bot hi","created_at":"t","agent_sessions":[{"agent":{"id":5,"username":"bot","account_type":"agent"},"stop_requested":null}]}"#;
    const SESSION_OK: &str =
        r#"{"agent":{"id":5,"username":"bot","account_type":"agent"},"stop_requested":null}"#;
    const SESSION_STOP: &str = r#"{"agent":{"id":5,"username":"bot","account_type":"agent"},"stop_requested":{"at":"t","by":{"id":2,"username":"alice","account_type":"human"}}}"#;
    const CONTEXT: &str = r#"{"area":{"name":"Ops","description":"Be brief"},"thread":{"title":"Deploy"},"target_id":7,"top_level":[{"id":7,"thread_id":9,"author":{"id":2,"username":"alice","account_type":"human"},"body":"@bot hi","created_at":"t"}],"chain":[]}"#;

    type SentLog = Arc<Mutex<Vec<(String, Option<AgentSender>)>>>;

    struct MockSession {
        tx: mpsc::Sender<ChatEvent>,
        script: Vec<ChatEvent>,
        on_abort: Vec<ChatEvent>,
        delay: Duration,
        sent: SentLog,
        aborts: Arc<AtomicUsize>,
        abort_hangs: bool,
        send_hangs: bool,
    }

    impl MockSession {
        fn run(&mut self, prompt: String, sender: Option<AgentSender>) {
            self.sent.lock().unwrap().push((prompt, sender));
            let tx = self.tx.clone();
            let script = self.script.clone();
            let delay = self.delay;
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                for event in script {
                    let _ = tx.send(event).await;
                }
            });
        }
    }

    impl ChatSession for MockSession {
        fn send_turn(&mut self, prompt: String) -> BoxFuture<'_, Result<()>> {
            self.run(prompt, None);
            let hangs = self.send_hangs;
            Box::pin(async move {
                if hangs {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
        fn send_turn_from(
            &mut self,
            prompt: String,
            sender: Option<AgentSender>,
        ) -> BoxFuture<'_, Result<()>> {
            self.run(prompt, sender);
            let hangs = self.send_hangs;
            Box::pin(async move {
                if hangs {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
        fn abort(&mut self) -> BoxFuture<'_, Result<()>> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            let tx = self.tx.clone();
            let events = self.on_abort.clone();
            let hangs = self.abort_hangs;
            Box::pin(async move {
                if hangs {
                    std::future::pending::<()>().await;
                }
                for event in events {
                    let _ = tx.send(event).await;
                }
                Ok(())
            })
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct MockOpener {
        queue: Mutex<Vec<ChatConn>>,
        dirs: Arc<Mutex<Vec<PathBuf>>>,
    }

    #[async_trait]
    impl SessionOpener for MockOpener {
        async fn open(&self, dir: &Path) -> Result<ChatConn> {
            self.dirs.lock().unwrap().push(dir.to_path_buf());
            self.queue
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| anyhow::anyhow!("no session /secret/open-path"))
        }
    }

    struct Probe {
        sent: SentLog,
        aborts: Arc<AtomicUsize>,
        dirs: Arc<Mutex<Vec<PathBuf>>>,
        shutdown: watch::Sender<bool>,
    }

    fn make_runtime(
        base: &str,
        script: Vec<ChatEvent>,
        on_abort: Vec<ChatEvent>,
        delay: Duration,
    ) -> (Runtime, Probe) {
        make_runtime_with(base, script, on_abort, delay, false)
    }

    fn make_runtime_with(
        base: &str,
        script: Vec<ChatEvent>,
        on_abort: Vec<ChatEvent>,
        delay: Duration,
        abort_hangs: bool,
    ) -> (Runtime, Probe) {
        build_runtime(base, script, on_abort, delay, abort_hangs, false)
    }

    fn build_runtime(
        base: &str,
        script: Vec<ChatEvent>,
        on_abort: Vec<ChatEvent>,
        delay: Duration,
        abort_hangs: bool,
        send_hangs: bool,
    ) -> (Runtime, Probe) {
        static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "threadit-test-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::SeqCst)
        ));
        let (tx, rx) = mpsc::channel(256);
        let sent = SentLog::default();
        let aborts = Arc::new(AtomicUsize::new(0));
        let dirs: Arc<Mutex<Vec<PathBuf>>> = Arc::default();
        let session = MockSession {
            tx,
            script,
            on_abort,
            delay,
            sent: sent.clone(),
            aborts: aborts.clone(),
            abort_hangs,
            send_hangs,
        };
        let opener = MockOpener {
            queue: Mutex::new(vec![ChatConn {
                session: Box::new(session),
                rx,
            }]),
            dirs: dirs.clone(),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let board = Board::new(base, "tok-123")
            .unwrap()
            .with_retry_delay(Duration::from_millis(1));
        let me = Account {
            id: 5,
            username: "bot".into(),
            account_type: "agent".into(),
        };
        let rt = Runtime::new(
            board,
            Box::new(opener),
            me,
            root.join("sessions"),
            Duration::from_millis(20),
            Duration::from_secs(3600),
            ShutdownToken::new(shutdown_rx),
        );
        (
            rt,
            Probe {
                sent,
                aborts,
                dirs,
                shutdown: shutdown_tx,
            },
        )
    }

    fn item() -> Message {
        serde_json::from_str(ITEM).unwrap()
    }

    fn base_handler(activity_session: &'static str, message_json: &'static str) -> Handler {
        Arc::new(
            move |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                ("GET", "/api/messages/7") => (200, message_json.into()),
                ("POST", "/api/messages/7/agent-activities") => (201, activity_session.into()),
                ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                _ => (404, "{}".into()),
            },
        )
    }

    fn json_bodies(server: &MockServer, suffix: &str) -> Vec<Value> {
        server
            .requests()
            .iter()
            .filter(|r| r.method == "POST" && r.path.ends_with(suffix))
            .map(|r| serde_json::from_str(&r.body).unwrap())
            .collect()
    }

    fn activities(server: &MockServer) -> Vec<(String, String, bool)> {
        json_bodies(server, "/agent-activities")
            .iter()
            .map(|v| {
                (
                    v["kind"].as_str().unwrap().to_string(),
                    v["body"].as_str().unwrap().to_string(),
                    v["ephemeral"].as_bool().unwrap_or(false),
                )
            })
            .collect()
    }

    fn replies(server: &MockServer) -> Vec<Value> {
        json_bodies(server, "/api/threads/9/messages")
    }

    fn emojis(server: &MockServer) -> Vec<String> {
        json_bodies(server, "/reactions")
            .iter()
            .map(|v| v["emoji"].as_str().unwrap().to_string())
            .collect()
    }

    fn assistant(text: &str) -> ChatEvent {
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: text.into(),
        }
    }

    fn finished_ok() -> ChatEvent {
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        }
    }

    fn tool(name: &str, args: &str) -> ChatEvent {
        ChatEvent::ToolCall {
            id: "t".into(),
            name: name.into(),
            args: args.into(),
        }
    }

    #[tokio::test]
    async fn answer_turn_maps_tools_replies_and_marks_done() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![
            tool("read", r#"{"path":"/etc/secret-file"}"#),
            tool("read", r#"{"path":"/other"}"#),
            ChatEvent::ToolOutput {
                id: "t".into(),
                text: "TOP-SECRET-OUTPUT".into(),
                is_error: false,
                done: true,
            },
            ChatEvent::Delta {
                role: ChatRole::Thinking,
                text: "private thoughts".into(),
            },
            tool("bash", r#"{"command":"cat /etc/passwd"}"#),
            ChatEvent::Error("raw /home/user/path failure".into()),
            assistant("Done "),
            assistant("here."),
            finished_ok(),
        ];
        let (mut rt, probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;

        let acts = activities(&server);
        let bodies: Vec<&str> = acts.iter().map(|a| a.1.as_str()).collect();
        assert_eq!(bodies, ["Started", "Reading files", "Running commands"]);
        assert!(!acts[0].2 && acts[1].2 && acts[2].2);
        assert_eq!(acts[1].0, "action");
        let all: String = server.requests().iter().map(|r| r.body.clone()).collect();
        for leak in [
            "secret-file",
            "/etc",
            "TOP-SECRET",
            "private thoughts",
            "/home/user",
            "tok-123",
        ] {
            assert!(!all.contains(leak), "leaked {leak}");
        }
        let reps = replies(&server);
        assert_eq!(reps.len(), 1);
        assert_eq!(reps[0]["body"], "Done here.");
        assert_eq!(reps[0]["parent_id"], 7);
        assert_eq!(emojis(&server), ["👀", "✅"]);

        let sent = probe.sent.lock().unwrap();
        assert!(sent[0].1.is_none());
        assert!(sent[0].0.contains("[TARGET MESSAGE]") && sent[0].0.contains("Be brief"));
        let dir = probe.dirs.lock().unwrap()[0].clone();
        assert!(dir.ends_with("sessions/thread-9"), "{dir:?}");
    }

    #[tokio::test]
    async fn agent_author_sets_loop_guard_sender() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (mut rt, probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        let mut it = item();
        it.author = Account {
            id: 8,
            username: "helper".into(),
            account_type: "agent".into(),
        };
        rt.handle_item(it, false).await;
        let sent = probe.sent.lock().unwrap();
        let sender = sent[0].1.as_ref().expect("sender");
        assert_eq!(sender.agent_id, "threadit:helper");
        assert_eq!(sender.hops, None);
        assert_eq!(replies(&server)[0]["body"], "(no response)");
    }

    #[tokio::test]
    async fn silent_turn_adds_done_only() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![
            ChatEvent::Silent {
                reason: None,
                text: "NO_REPLY".into(),
            },
            finished_ok(),
        ];
        let (mut rt, _probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert!(replies(&server).is_empty());
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn failed_turn_reports_safely_and_keeps_working_mark() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![ChatEvent::TurnFinished {
            ok: false,
            error: Some("boom /root/key".into()),
        }];
        let (mut rt, _probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        let acts = activities(&server);
        assert_eq!(
            acts.last().unwrap(),
            &("error".into(), "Run failed: backend failed".into(), false)
        );
        let reps = replies(&server);
        assert_eq!(reps[0]["body"], FAILURE_REPLY);
        assert_eq!(emojis(&server), ["👀"]);
        let all: String = server.requests().iter().map(|r| r.body.clone()).collect();
        assert!(!all.contains("boom") && !all.contains("/root"));
    }

    #[tokio::test]
    async fn closed_session_drops_session_and_fails() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![ChatEvent::SessionClosed {
            error: Some("x".into()),
        }];
        let (mut rt, _probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert!(rt.sessions.is_empty());
        assert_eq!(
            activities(&server).last().unwrap().1,
            "Run failed: backend closed"
        );
    }

    #[tokio::test]
    async fn stop_in_started_response_replies_and_never_starts_backend() {
        let server = MockServer::start(base_handler(SESSION_STOP, ITEM)).await;
        let (mut rt, probe) = make_runtime(
            &server.base,
            vec![tool("bash", "{}")],
            vec![],
            Duration::ZERO,
        );
        rt.handle_item(item(), false).await;
        assert!(probe.sent.lock().unwrap().is_empty());
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn stop_in_resumed_response_replies_and_never_starts_backend() {
        let server = MockServer::start(base_handler(SESSION_STOP, ITEM)).await;
        let (mut rt, probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        rt.handle_item(item(), true).await;
        assert!(probe.sent.lock().unwrap().is_empty());
        assert_eq!(activities(&server).len(), 1);
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    /// Activity handler that answers with a stop only for one activity kind.
    fn stop_on_kind(kind: &'static str) -> Handler {
        Arc::new(
            move |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                ("GET", "/api/messages/7") => (200, ITEM.into()),
                ("POST", "/api/messages/7/agent-activities") => {
                    let hit = req.body.contains(&format!("\"kind\":\"{kind}\""));
                    (201, if hit { SESSION_STOP } else { SESSION_OK }.into())
                }
                ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                _ => (404, "{}".into()),
            },
        )
    }

    fn aborted() -> Vec<ChatEvent> {
        vec![ChatEvent::TurnFinished {
            ok: false,
            error: Some("aborted".into()),
        }]
    }

    #[tokio::test]
    async fn stop_in_question_response_stops_the_item() {
        let server = MockServer::start(stop_on_kind("elicitation")).await;
        let script = vec![ChatEvent::QuestionAsked {
            request_id: "q".into(),
            questions: vec![],
        }];
        let (mut rt, probe) = make_runtime(&server.base, script, aborted(), Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        let reps = replies(&server);
        assert_eq!(reps.len(), 1);
        assert_eq!(reps[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn stop_in_tool_response_stops_the_item() {
        let server = MockServer::start(stop_on_kind("action")).await;
        let (mut rt, probe) = make_runtime(
            &server.base,
            vec![tool("bash", "{}")],
            aborted(),
            Duration::ZERO,
        );
        rt.handle_item(item(), false).await;
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn stop_in_heartbeat_response_stops_the_item() {
        // "Started" is a thought too, so stop only on the heartbeat text.
        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("GET", "/api/messages/7") => (200, ITEM.into()),
                    ("POST", "/api/messages/7/agent-activities") => {
                        let beat = req.body.contains("Still working");
                        (201, if beat { SESSION_STOP } else { SESSION_OK }.into())
                    }
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let (rt, probe) = make_runtime(&server.base, vec![], aborted(), Duration::ZERO);
        let mut rt = rt.with_heartbeat(Duration::from_millis(40));
        rt.handle_item(item(), false).await;
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn stop_request_seen_by_polling_aborts_the_turn() {
        let stopped: &'static str = ITEM_STOPPED;
        let handler: Handler =
            Arc::new(
                move |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7") => (200, stopped.into()),
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let on_abort = vec![ChatEvent::TurnFinished {
            ok: false,
            error: Some("aborted".into()),
        }];
        // The script ends without a TurnFinished: only the stop poll can end the turn.
        let (mut rt, probe) = make_runtime(
            &server.base,
            vec![assistant("partial")],
            on_abort,
            Duration::ZERO,
        );
        rt.handle_item(item(), false).await;
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        let reps = replies(&server);
        assert_eq!(reps.len(), 1);
        assert_eq!(reps[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
        assert_eq!(rt.sessions.len(), 1);
    }

    const ITEM_STOPPED: &str = r#"{"id":7,"thread_id":9,"author":{"id":2,"username":"alice","account_type":"human"},"body":"x","created_at":"t","agent_sessions":[{"agent":{"id":5,"username":"bot","account_type":"agent"},"stop_requested":{"at":"t","by":{"id":2,"username":"alice","account_type":"human"}}}]}"#;

    #[tokio::test]
    async fn item_with_stop_request_gets_done_and_is_skipped() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (mut rt, probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        let stopped: Message = serde_json::from_str(ITEM_STOPPED).unwrap();
        rt.handle_item(stopped, false).await;
        assert_eq!(emojis(&server), ["✅"]);
        assert!(activities(&server).is_empty());
        assert!(probe.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn question_posts_elicitation_aborts_and_keeps_working_mark() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![ChatEvent::QuestionAsked {
            request_id: "q1".into(),
            questions: vec![QuestionInfo {
                header: "h".into(),
                question: "Which env?".into(),
                options: vec![QuestionOption {
                    label: "prod".into(),
                    description: "live".into(),
                }],
                multiple: false,
                custom: false,
            }],
        }];
        let on_abort = vec![ChatEvent::TurnFinished {
            ok: false,
            error: Some("aborted".into()),
        }];
        let (mut rt, probe) = make_runtime(&server.base, script, on_abort, Duration::ZERO);
        rt.handle_item(item(), false).await;
        let acts = activities(&server);
        let elicit = acts.iter().find(|a| a.0 == "elicitation").unwrap();
        assert!(elicit.1.contains("Which env?") && elicit.1.contains("- prod: live"));
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert!(replies(&server)[0]["body"]
            .as_str()
            .unwrap()
            .contains("Which env?"));
        assert_eq!(emojis(&server), ["👀"]);
        assert_eq!(rt.sessions.len(), 1);
    }

    #[tokio::test]
    async fn closed_thread_stops_writes_and_aborts() {
        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("GET", "/api/messages/7") => (200, ITEM.into()),
                    ("POST", "/api/messages/7/agent-activities") => (
                        409,
                        r#"{"error":{"code":"thread_closed","message":"x"}}"#.into(),
                    ),
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let (mut rt, probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert!(replies(&server).is_empty());
        assert_eq!(emojis(&server), ["👀"]);
        assert!(probe.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn heartbeat_posts_ephemeral_still_working() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (rt, _probe) = make_runtime(
            &server.base,
            vec![assistant("ok"), finished_ok()],
            vec![],
            Duration::from_millis(250),
        );
        let mut rt = rt.with_heartbeat(Duration::from_millis(60));
        rt.handle_item(item(), false).await;
        let acts = activities(&server);
        assert!(
            acts.iter()
                .any(|a| a.0 == "thought" && a.1 == "Still working" && a.2),
            "{acts:?}"
        );
    }

    #[tokio::test]
    async fn resumed_item_posts_resumed_thought_first() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (mut rt, _probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        rt.handle_item(item(), true).await;
        let bodies: Vec<String> = activities(&server).into_iter().map(|a| a.1).collect();
        assert_eq!(bodies[0], "Resumed after restart");
        assert!(!bodies.iter().any(|b| b == "Started"));
    }

    #[tokio::test]
    async fn poll_skips_handled_items() {
        let page = format!(r#"{{"items":[{ITEM}],"has_more":false}}"#);
        let handler: Handler = Arc::new(move |req: &Req, _| {
            match (req.method.as_str(), req.path.split('?').next().unwrap()) {
                ("GET", "/api/attention") => (200, page.clone()),
                ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                _ => (404, "{}".into()),
            }
        });
        let server = MockServer::start(handler).await;
        let (mut rt, probe) = make_runtime(
            &server.base,
            vec![assistant("a"), finished_ok()],
            vec![],
            Duration::ZERO,
        );
        rt.poll_once().await;
        rt.poll_once().await;
        assert_eq!(probe.sent.lock().unwrap().len(), 1);
        let first = server
            .requests()
            .into_iter()
            .find(|r| r.path.starts_with("/api/attention"))
            .unwrap();
        assert!(first
            .path
            .contains("status=unacknowledged&order=asc&limit=50"));
    }

    #[tokio::test]
    async fn run_exits_on_rejected_token_and_non_agent() {
        for (status, body) in [
            (401, r#"{"error":{"code":"invalid_token","message":"x"}}"#),
            (200, r#"{"id":1,"username":"u","account_type":"human"}"#),
        ] {
            let handler: Handler = Arc::new(move |_, _| (status, body.into()));
            let server = MockServer::start(handler).await;
            let (_tx, rx) = watch::channel(false);
            let board = Board::new(&server.base, "tok").unwrap();
            let opener = MockOpener {
                queue: Mutex::new(vec![]),
                dirs: Arc::default(),
            };
            tokio::time::timeout(
                Duration::from_secs(5),
                run(
                    board,
                    Box::new(opener),
                    PathBuf::from("/tmp/x"),
                    Duration::from_millis(20),
                    Duration::from_secs(3600),
                    ShutdownToken::new(rx),
                ),
            )
            .await
            .expect("run must return");
        }
    }

    fn with_bounds(rt: Runtime, timeout_ms: u64, stop_ms: u64) -> Runtime {
        rt.with_turn_timeout(Duration::from_millis(timeout_ms))
            .with_stop_wait(Duration::from_millis(stop_ms))
    }

    #[tokio::test]
    async fn missing_completion_hits_deadline_and_drops_session() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (rt, probe) = make_runtime_with(&server.base, vec![], vec![], Duration::ZERO, true);
        let mut rt = with_bounds(rt, 100, 100);
        tokio::time::timeout(Duration::from_secs(5), rt.handle_item(item(), false))
            .await
            .expect("the item must end");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert!(rt.sessions.is_empty());
        assert_eq!(
            activities(&server).last().unwrap().1,
            "Run failed: timed out"
        );
        assert_eq!(replies(&server)[0]["body"], FAILURE_REPLY);
        assert_eq!(emojis(&server), ["👀"]);
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_on_a_hung_abort() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (rt, probe) = make_runtime_with(&server.base, vec![], vec![], Duration::ZERO, true);
        let mut rt = with_bounds(rt, 60_000, 100);
        probe.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), rt.handle_item(item(), false))
            .await
            .expect("shutdown must end the item");
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 1);
        assert!(rt.sessions.is_empty());
        assert!(replies(&server).is_empty());
    }

    fn started(origin: TurnOrigin) -> ChatEvent {
        ChatEvent::TurnStarted { origin }
    }

    #[tokio::test]
    async fn autonomous_completion_does_not_finish_the_item() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![
            started(TurnOrigin::Autonomous),
            assistant("AUTONOMOUS"),
            finished_ok(),
            started(TurnOrigin::Submitted),
            assistant("Real answer"),
            finished_ok(),
        ];
        let (mut rt, _probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        let reps = replies(&server);
        assert_eq!(reps.len(), 1);
        assert_eq!(reps[0]["body"], "Real answer");
    }

    #[tokio::test]
    async fn autonomous_completion_does_not_satisfy_the_abort_drain() {
        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7") => (200, ITEM_STOPPED.into()),
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let on_abort = vec![started(TurnOrigin::Autonomous), finished_ok()];
        let (rt, _probe) = make_runtime(&server.base, vec![], on_abort, Duration::ZERO);
        let mut rt = with_bounds(rt, 60_000, 150);
        rt.handle_item(item(), false).await;
        // The drain saw only an autonomous finish, so the session is not usable.
        assert!(rt.sessions.is_empty());
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
        assert_eq!(emojis(&server), ["👀", "✅"]);
    }

    #[tokio::test]
    async fn autonomous_question_does_not_end_the_item() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let script = vec![
            started(TurnOrigin::Autonomous),
            ChatEvent::QuestionAsked {
                request_id: "q".into(),
                questions: vec![],
            },
            tool("read", r#"{"path":"/x"}"#),
            finished_ok(),
            started(TurnOrigin::Submitted),
            assistant("Real answer"),
            finished_ok(),
        ];
        let (mut rt, probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert_eq!(probe.aborts.load(Ordering::SeqCst), 0);
        assert!(activities(&server)
            .iter()
            .all(|(kind, _, _)| kind != "elicitation" && kind != "action"));
        assert_eq!(replies(&server)[0]["body"], "Real answer");
    }

    #[tokio::test]
    async fn autonomous_start_consumed_before_abort_keeps_drain_waiting() {
        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7") => (200, ITEM_STOPPED.into()),
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let script = vec![started(TurnOrigin::Autonomous)];
        let (rt, _probe) = make_runtime(&server.base, script, vec![finished_ok()], Duration::ZERO);
        let mut rt = with_bounds(rt, 60_000, 150);
        rt.handle_item(item(), false).await;
        assert!(rt.sessions.is_empty());
        assert_eq!(replies(&server)[0]["body"], STOPPED_REPLY);
    }

    #[tokio::test(start_paused = true)]
    async fn hung_send_hits_the_deadline_and_closes_the_session() {
        let server = MockServer::start(base_handler(SESSION_OK, ITEM)).await;
        let (rt, _probe) = build_runtime(&server.base, vec![], vec![], Duration::ZERO, false, true);
        let mut rt = with_bounds(rt, 100, 100);
        rt.handle_item(item(), false).await;
        assert!(rt.sessions.is_empty());
        assert_eq!(
            activities(&server).last().unwrap().1,
            "Run failed: timed out"
        );
        assert_eq!(replies(&server)[0]["body"], FAILURE_REPLY);
    }

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn logs_hold_no_raw_backend_open_or_server_text() {
        let buf = LogBuf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                    ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (
                        400,
                        r#"{"error":{"code":"leak-/secret/server-code","message":"x"}}"#.into(),
                    ),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let script = vec![
            ChatEvent::Error("raw /home/user/backend-failure".into()),
            assistant("answer"),
            finished_ok(),
        ];
        let (mut rt, _probe) = make_runtime(&server.base, script, vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        // The session stays cached. Clear it so the next item hits the open error.
        rt.close_all().await;
        rt.handle_item(item(), false).await;

        let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("threadit backend reported an error"),
            "{logs}"
        );
        assert!(
            logs.contains("threadit could not open a chat session"),
            "{logs}"
        );
        assert!(logs.contains("board returned 400 other"), "{logs}");
        for leak in ["/home/user", "backend-failure", "/secret", "leak-"] {
            assert!(!logs.contains(leak), "log leaked {leak}: {logs}");
        }
    }

    fn item_json(state: &str, reply_id: &str) -> String {
        format!(
            r#"{{"id":7,"thread_id":9,"author":{{"id":2,"username":"alice","account_type":"human"}},"body":"x","created_at":"t","agent_sessions":[{{"agent":{{"id":5,"username":"bot","account_type":"agent"}},"stop_requested":null,"state":"{state}","reply_id":{reply_id}}}]}}"#
        )
    }

    /// Run `resume_processing` for one item. Returns (turns started, replies, emojis).
    async fn resume_case(state: &str, reply_id: &str) -> (usize, Vec<String>, Vec<String>) {
        resume_case_marked(state, reply_id, false).await
    }

    async fn resume_case_marked(
        state: &str,
        reply_id: &str,
        marker: bool,
    ) -> (usize, Vec<String>, Vec<String>) {
        let page = format!(
            r#"{{"items":[{}],"has_more":false}}"#,
            item_json(state, reply_id)
        );
        let handler: Handler = Arc::new(move |req: &Req, _| {
            match (req.method.as_str(), req.path.split('?').next().unwrap()) {
                ("GET", "/api/attention") => (200, page.clone()),
                ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                _ => (404, "{}".into()),
            }
        });
        let server = MockServer::start(handler).await;
        let (mut rt, probe) = make_runtime(
            &server.base,
            vec![assistant("fresh"), finished_ok()],
            vec![],
            Duration::ZERO,
        );
        if marker {
            rt.shared.write_marker(7).unwrap();
        }
        rt.resume_processing().await;
        assert_eq!(rt.shared.has_marker(7), marker && reply_id == "null");
        let turns = probe.sent.lock().unwrap().len();
        let bodies = replies(&server)
            .iter()
            .map(|v| v["body"].as_str().unwrap().to_string())
            .collect();
        (turns, bodies, emojis(&server))
    }

    #[tokio::test]
    async fn resume_runs_only_working_and_stale_items() {
        for state in ["working", "stale"] {
            let (turns, bodies, emojis) = resume_case(state, "null").await;
            assert_eq!(turns, 1, "{state}");
            assert_eq!(bodies, ["fresh"]);
            assert_eq!(emojis, ["👀", "✅"]);
        }
        for state in ["needs_input", "error", "queued", "done"] {
            let (turns, bodies, emojis) = resume_case(state, "null").await;
            assert_eq!(turns, 0, "{state}");
            assert!(bodies.is_empty() && emojis.is_empty(), "{state}");
        }
    }

    #[tokio::test]
    async fn resume_finishes_stopping_items_without_a_turn() {
        let (turns, bodies, emojis) = resume_case("stopping", "null").await;
        assert_eq!(turns, 0);
        assert_eq!(bodies, [STOPPED_REPLY]);
        assert_eq!(emojis, ["✅"]);
    }

    #[tokio::test]
    async fn resume_with_existing_reply_only_adds_done() {
        for state in ["working", "stale", "stopping"] {
            let (turns, bodies, emojis) = resume_case(state, "31").await;
            assert_eq!(turns, 0, "{state}");
            assert!(bodies.is_empty(), "{state}");
            assert!(
                emojis.iter().all(|e| e == "✅") && !emojis.is_empty(),
                "{state}"
            );
        }
    }

    #[tokio::test]
    async fn resume_with_delivery_marker_never_runs_or_replies() {
        let (turns, bodies, emojis) = resume_case_marked("working", "null", true).await;
        assert_eq!(turns, 0);
        assert!(bodies.is_empty());
        assert!(emojis.is_empty(), "item stays at working");
        let (turns, bodies, emojis) = resume_case_marked("working", "31", true).await;
        assert_eq!(turns, 0);
        assert!(bodies.is_empty());
        assert_eq!(emojis, ["✅"]);
    }

    #[tokio::test]
    async fn uncertain_reply_leaves_marker_and_done_removes_it() {
        let handler = uncertain_reply_handler(item_json("working", "null"));
        let server = MockServer::start(handler).await;
        let (mut rt, _probe) = make_runtime(
            &server.base,
            vec![assistant("answer"), finished_ok()],
            vec![],
            Duration::ZERO,
        );
        rt.handle_item(item(), false).await;
        assert!(rt.shared.has_marker(7));
        rt.shared.mark_done(&item()).await;
        assert!(!rt.shared.has_marker(7));
    }

    fn uncertain_reply_handler(read_back: String) -> Handler {
        Arc::new(
            move |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                ("GET", "/api/messages/7/context") => (200, CONTEXT.into()),
                ("GET", "/api/messages/7") => (200, read_back.clone()),
                ("POST", "/api/messages/7/agent-activities") => (201, SESSION_OK.into()),
                ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                ("POST", "/api/threads/9/messages") => (500, "{}".into()),
                _ => (404, "{}".into()),
            },
        )
    }

    #[tokio::test]
    async fn uncertain_reply_adds_done_only_when_reply_id_is_set() {
        for (reply_id, expect) in [("31", vec!["👀", "✅"]), ("null", vec!["👀"])] {
            let handler = uncertain_reply_handler(item_json("working", reply_id));
            let server = MockServer::start(handler).await;
            let (mut rt, _probe) = make_runtime(
                &server.base,
                vec![assistant("answer"), finished_ok()],
                vec![],
                Duration::ZERO,
            );
            rt.handle_item(item(), false).await;
            assert_eq!(replies(&server).len(), 1, "never resend");
            assert_eq!(emojis(&server), expect, "reply_id {reply_id}");
        }
    }

    #[tokio::test]
    async fn failure_path_does_not_reply_after_thread_closed() {
        let handler: Handler =
            Arc::new(
                |req: &Req, _| match (req.method.as_str(), req.path.as_str()) {
                    ("GET", "/api/messages/7/context") => (500, "{}".into()),
                    ("POST", "/api/messages/7/agent-activities") => {
                        if req.body.contains(r#""kind":"error""#) {
                            (
                                409,
                                r#"{"error":{"code":"thread_closed","message":"x"}}"#.into(),
                            )
                        } else {
                            (201, SESSION_OK.into())
                        }
                    }
                    ("POST", "/api/messages/7/reactions") => (200, ITEM.into()),
                    ("POST", "/api/threads/9/messages") => (201, ITEM.into()),
                    _ => (404, "{}".into()),
                },
            );
        let server = MockServer::start(handler).await;
        let (mut rt, _probe) =
            make_runtime(&server.base, vec![finished_ok()], vec![], Duration::ZERO);
        rt.handle_item(item(), false).await;
        assert!(replies(&server).is_empty());
        assert!(activities(&server).iter().any(|a| a.0 == "error"));
    }

    #[test]
    fn question_text_without_questions_has_fallback() {
        assert_eq!(question_text(&[]), "The agent needs more input.");
    }
}
