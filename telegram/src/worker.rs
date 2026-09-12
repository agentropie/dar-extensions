//! A generation's continuously consumed backend stream, independent of polling.
use super::*;
use crate::ack::AckGuard;
use crate::stream::{LiveTurn, RealClock};
use dar_extension_sdk::chat::{ChatRole, TurnOrigin};
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

const INTAKE_LIMIT: usize = 16;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(40);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

struct Input {
    message_id: i64,
    text: String,
    permit: OwnedSemaphorePermit,
}

pub(super) struct Worker {
    tx: mpsc::Sender<Input>,
    capacity: Arc<Semaphore>,
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Worker {
    pub fn start(
        ctx: StartCtx,
        session_dir: PathBuf,
        backend: Option<String>,
        bot: Arc<dyn BotApi>,
        api: TelegramStreamApi,
    ) -> Self {
        let (tx, mut input) = mpsc::channel(INTAKE_LIMIT);
        let (cancel, mut cancelled) = watch::channel(false);
        let stop = cancel.clone();
        let mut shutdown = ctx.shutdown.clone();
        let task = tokio::spawn(async move {
            let opened = tokio::select! {
                _ = cancelled.changed() => return,
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(OPERATION_TIMEOUT, open_session(&ctx, &session_dir, backend.as_deref())) => result,
            };
            match opened {
                Ok(Ok(conn)) => {
                    let connection = run_connection(conn, &mut input, &mut cancelled, bot, &api);
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = &mut connection => {},
                        _ = shutdown.cancelled() => {
                            let _ = stop.send(true);
                            connection.await;
                        }
                    }
                }
                _ => {
                    tokio::select! {
                        _ = cancelled.changed() => {},
                        _ = shutdown.cancelled() => {},
                        _ = tokio::time::timeout(OPERATION_TIMEOUT, api.send("Failed to start agent session; please retry.")) => {},
                    }
                }
            }
        });
        Self {
            tx,
            capacity: Arc::new(Semaphore::new(INTAKE_LIMIT)),
            cancel,
            task,
        }
    }

    pub fn submit(&self, message_id: i64, text: String) -> bool {
        let Ok(permit) = Arc::clone(&self.capacity).try_acquire_owned() else {
            return false;
        };
        self.tx
            .try_send(Input {
                message_id,
                text,
                permit,
            })
            .is_ok()
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    pub async fn stop(mut self) {
        self.cancel();
        if tokio::time::timeout(CLOSE_TIMEOUT + Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = self.task.await;
        }
    }
}

#[async_trait]
trait ReplyApi: StreamApi {
    fn chat_id(&self) -> i64;
    async fn finalize(&self, outcome: &TurnOutcome) -> Result<()>;
}

#[async_trait]
impl ReplyApi for TelegramStreamApi {
    fn chat_id(&self) -> i64 {
        self.chat_id
    }
    async fn finalize(&self, outcome: &TurnOutcome) -> Result<()> {
        finalize_reply(&self.client, &self.base, self.chat_id, outcome).await
    }
}

async fn run_connection(
    mut conn: ChatConn,
    input: &mut mpsc::Receiver<Input>,
    cancelled: &mut watch::Receiver<bool>,
    bot: Arc<dyn BotApi>,
    api: &impl ReplyApi,
) {
    let (submit, mut submissions) = mpsc::channel(INTAKE_LIMIT);
    // Submission can await acceptance while the bounded event channel fills.
    // Poll it concurrently with rendering so neither side can deadlock the other.
    let result = {
        let drive = async {
            while let Some(text) = submissions.recv().await {
                tokio::time::timeout(OPERATION_TIMEOUT, conn.session.send_turn(text))
                    .await
                    .context("agent submission timed out")??;
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {
            biased;
            _ = cancelled.changed() => Ok(()),
            result = drive => result,
            result = render_events(&mut conn.rx, input, submit, bot, api) => result,
        }
    };
    // Cancellation drops the renderer and all pending guards before closing.
    // No obsolete generation can start a new send after stop() returns.
    if let Err(err) = result {
        tracing::warn!(error = %err, "telegram chat worker stopped");
        tokio::select! {
            biased;
            _ = cancelled.changed() => {},
            _ = tokio::time::timeout(OPERATION_TIMEOUT, api.send("Agent session stopped; pending messages were cancelled. Please retry.")) => {},
        }
    }
    if tokio::time::timeout(CLOSE_TIMEOUT, conn.session.close())
        .await
        .is_err()
    {
        tracing::warn!("telegram backend close timed out");
    }
}

struct Pending {
    guard: AckGuard,
    _permit: OwnedSemaphorePermit,
}

async fn render_events(
    events: &mut mpsc::Receiver<ChatEvent>,
    input: &mut mpsc::Receiver<Input>,
    submit: mpsc::Sender<String>,
    bot: Arc<dyn BotApi>,
    api: &impl ReplyApi,
) -> Result<()> {
    let mut pending = VecDeque::new();
    let mut active = None;
    let mut live = LiveTurn::new(api, RealClock);
    let mut origin = None;
    loop {
        tokio::select! {
            inbound = input.recv() => {
                let Some(inbound) = inbound else { return Ok(()); };
                let guard = AckGuard::start(Arc::clone(&bot), api.chat_id(), inbound.message_id).await;
                pending.push_back(Pending { guard, _permit: inbound.permit });
                submit.try_send(inbound.text).context("submission queue closed")?;
            }
            event = events.recv() => {
                let event = event.context("backend event stream closed")?;
                if let ChatEvent::SessionClosed { error } = event {
                    bail!("backend closed: {}", error.unwrap_or_default());
                }
                if let ChatEvent::TurnStarted { origin: started } = event {
                    origin = Some(started);
                    if started == TurnOrigin::Submitted { active = pending.pop_front(); }
                    continue;
                }
                // Legacy backends emit only submitted turns, with no start event.
                if origin.is_none() && matches!(event, ChatEvent::Delta { .. } | ChatEvent::ToolCall { .. } | ChatEvent::TurnFinished { .. }) {
                    origin = Some(TurnOrigin::Submitted);
                    active = pending.pop_front();
                }
                match event {
                    ChatEvent::Delta { role: ChatRole::Assistant, text } => live.push_text(&text).await,
                    ChatEvent::ToolCall { name, args, .. } => live.tool_started(&name, &args).await,
                    ChatEvent::TurnFinished { ok, error } => {
                        live.finish().await;
                        let reply = if !live.answer().trim().is_empty() {
                            live.answer().to_string()
                        } else if !ok {
                            format!("(turn failed: {})", error.unwrap_or_else(|| "unknown".into()))
                        } else if origin == Some(TurnOrigin::Autonomous) {
                            String::new()
                        } else { "(no response)".into() };
                        let outcome = TurnOutcome { reply, answer_msg: live.answer_message_id() };
                        if !outcome.reply.is_empty() {
                            match tokio::time::timeout(OPERATION_TIMEOUT, api.finalize(&outcome)).await {
                                Ok(Ok(())) => {},
                                result => tracing::warn!(error = ?result, "telegram final reply delivery failed"),
                            }
                        }
                        if let Some(pending) = active.take() { pending.guard.finish().await; }
                        live = LiveTurn::new(api, RealClock);
                        origin = None;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dar_extension_sdk::chat::BoxFuture;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Api {
        replies: Mutex<Vec<String>>,
        reactions: Mutex<Vec<(i64, bool)>>,
        changed: tokio::sync::Notify,
        hang_final: std::sync::atomic::AtomicBool,
    }
    #[async_trait]
    impl BotApi for Api {
        async fn set_reaction(&self, _: i64, id: i64, emoji: Option<&str>) {
            self.reactions.lock().unwrap().push((id, emoji.is_some()));
            self.changed.notify_one();
        }
        async fn send_chat_action(&self, _: i64, _: &str) {}
    }
    #[async_trait]
    impl StreamApi for Api {
        async fn send(&self, _: &str) -> Option<i64> {
            Some(1)
        }
        async fn edit(&self, _: i64, _: &str) -> EditResult {
            EditResult::Ok
        }
    }
    #[async_trait]
    impl ReplyApi for Api {
        fn chat_id(&self) -> i64 {
            1
        }
        async fn finalize(&self, outcome: &TurnOutcome) -> Result<()> {
            if self.hang_final.load(std::sync::atomic::Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            self.replies.lock().unwrap().push(outcome.reply.clone());
            self.changed.notify_one();
            Ok(())
        }
    }
    struct Backend {
        submissions: mpsc::UnboundedSender<String>,
        closed: Arc<std::sync::atomic::AtomicBool>,
        hang: bool,
    }
    impl ChatSession for Backend {
        fn send_turn(&mut self, text: String) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.submissions.send(text).unwrap();
                if self.hang {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
        fn abort(&mut self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                self.closed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            })
        }
    }
    struct Fixture {
        input: mpsc::Sender<Input>,
        events: mpsc::Sender<ChatEvent>,
        submitted: mpsc::UnboundedReceiver<String>,
        api: Arc<Api>,
        cancel: watch::Sender<bool>,
        closed: Arc<std::sync::atomic::AtomicBool>,
        task: JoinHandle<()>,
    }
    impl Fixture {
        fn new(hang: bool) -> Self {
            let (input, mut rx) = mpsc::channel(INTAKE_LIMIT);
            let (events, event_rx) = mpsc::channel(8);
            let (submissions, submitted) = mpsc::unbounded_channel();
            let (cancel, mut cancelled) = watch::channel(false);
            let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let conn = ChatConn {
                session: Box::new(Backend {
                    submissions,
                    closed: closed.clone(),
                    hang,
                }),
                rx: event_rx,
            };
            let api = Arc::new(Api::default());
            let sink = api.clone();
            let task = tokio::spawn(async move {
                run_connection(conn, &mut rx, &mut cancelled, sink.clone(), &*sink).await;
            });
            Self {
                input,
                events,
                submitted,
                api,
                cancel,
                closed,
                task,
            }
        }
        async fn submit(&mut self, id: i64, text: &str) {
            self.input
                .send(Input {
                    message_id: id,
                    text: text.into(),
                    permit: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
                })
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), self.submitted.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                text
            );
        }
        async fn start(&self, origin: TurnOrigin) {
            self.events
                .send(ChatEvent::TurnStarted { origin })
                .await
                .unwrap();
        }
        async fn finish(&self, text: &str) {
            self.events
                .send(ChatEvent::Delta {
                    role: ChatRole::Assistant,
                    text: text.into(),
                })
                .await
                .unwrap();
            self.events
                .send(ChatEvent::TurnFinished {
                    ok: true,
                    error: None,
                })
                .await
                .unwrap();
        }
        async fn replies(&self, count: usize) -> Vec<String> {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let notification = self.api.changed.notified();
                    let replies = self.api.replies.lock().unwrap().clone();
                    if replies.len() >= count {
                        return replies;
                    }
                    notification.await;
                }
            })
            .await
            .unwrap()
        }
        async fn stop(self) {
            self.cancel.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(1), self.task)
                .await
                .unwrap()
                .unwrap();
            assert!(self.closed.load(std::sync::atomic::Ordering::Relaxed));
        }
    }

    #[tokio::test]
    async fn proactive_reply_arrives_without_input_and_later_replies_stay_aligned() {
        let mut f = Fixture::new(false);
        f.submit(1, "A").await;
        f.start(TurnOrigin::Submitted).await;
        f.finish("answer A").await;
        assert_eq!(f.replies(1).await, ["answer A"]);
        f.start(TurnOrigin::Autonomous).await;
        f.finish("background B").await;
        assert_eq!(f.replies(2).await, ["answer A", "background B"]);
        for (id, text) in [(2, "C"), (3, "D")] {
            f.submit(id, text).await;
            f.start(TurnOrigin::Submitted).await;
            f.finish(text).await;
            f.replies(id as usize + 1).await;
        }
        assert_eq!(f.replies(4).await, ["answer A", "background B", "C", "D"]);
        assert_eq!(
            *f.api.reactions.lock().unwrap(),
            [
                (1, true),
                (1, false),
                (2, true),
                (2, false),
                (3, true),
                (3, false)
            ]
        );
        f.stop().await;
    }

    #[tokio::test]
    async fn autonomous_completion_does_not_clear_queued_acknowledgements() {
        let mut f = Fixture::new(false);
        f.start(TurnOrigin::Autonomous).await;
        f.submit(2, "same text").await;
        f.submit(3, "same text").await;
        f.finish("background").await;
        f.replies(1).await;
        assert_eq!(*f.api.reactions.lock().unwrap(), [(2, true), (3, true)]);
        f.start(TurnOrigin::Submitted).await;
        f.finish("first").await;
        f.replies(2).await;
        assert_eq!(
            *f.api.reactions.lock().unwrap(),
            [(2, true), (3, true), (2, false)]
        );
        f.start(TurnOrigin::Submitted).await;
        f.finish("second").await;
        assert_eq!(f.replies(3).await, ["background", "first", "second"]);
        f.stop().await;
    }

    #[tokio::test]
    async fn empty_autonomous_turn_is_silent_and_legacy_turn_still_delivers() {
        let mut f = Fixture::new(false);
        f.start(TurnOrigin::Autonomous).await;
        f.finish(" \n").await;
        f.submit(1, "legacy").await;
        f.finish("legacy reply").await;
        assert_eq!(f.replies(1).await, ["legacy reply"]);
        f.stop().await;
    }

    #[tokio::test]
    async fn cancellation_closes_backend_during_hung_submission() {
        let mut f = Fixture::new(true);
        f.submit(1, "hang").await;
        f.start(TurnOrigin::Autonomous).await;
        f.finish("still consuming events").await;
        assert_eq!(f.replies(1).await, ["still consuming events"]);
        f.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn hung_final_delivery_times_out_and_later_turn_progresses() {
        let f = Fixture::new(false);
        f.api
            .hang_final
            .store(true, std::sync::atomic::Ordering::Relaxed);
        f.start(TurnOrigin::Autonomous).await;
        f.finish("stuck delivery").await;
        tokio::task::yield_now().await;
        tokio::time::advance(OPERATION_TIMEOUT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        f.api
            .hang_final
            .store(false, std::sync::atomic::Ordering::Relaxed);
        f.start(TurnOrigin::Autonomous).await;
        f.finish("next delivery").await;
        assert_eq!(f.replies(1).await, ["next delivery"]);
        f.stop().await;
    }
    #[tokio::test]
    async fn proactive_turn_uses_real_http_stream_and_markdown_finalization() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, mut received) = mpsc::unbounded_channel();
        let server = std::thread::spawn(move || {
            for connection in listener.incoming() {
                let mut connection = connection.unwrap();
                connection
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut byte = [0];
                    connection.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                    if bytes.ends_with(b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(str::to_owned)
                            })
                            .unwrap()
                            .parse::<usize>()
                            .unwrap();
                        break (bytes.len(), length);
                    }
                };
                bytes.resize(header_end + length, 0);
                connection.read_exact(&mut bytes[header_end..]).unwrap();
                let payload: Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
                let final_reply = payload["parse_mode"] == "MarkdownV2";
                sent.send(payload).unwrap();
                let body = r#"{"ok":true,"result":{"message_id":42}}"#;
                write!(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                if final_reply {
                    return;
                }
            }
        });
        let (events, rx) = mpsc::channel(8);
        let (_input, mut input) = mpsc::channel(INTAKE_LIMIT);
        let (submissions, _submitted) = mpsc::unbounded_channel();
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let conn = ChatConn {
            session: Box::new(Backend {
                submissions,
                closed: closed.clone(),
                hang: false,
            }),
            rx,
        };
        let (cancel, mut cancelled) = watch::channel(false);
        let api = TelegramStreamApi {
            client: reqwest::Client::new(),
            base: format!("http://{address}"),
            chat_id: 123,
        };
        let task = tokio::spawn(async move {
            run_connection(
                conn,
                &mut input,
                &mut cancelled,
                Arc::new(Api::default()),
                &api,
            )
            .await;
        });
        events
            .send(ChatEvent::TurnStarted {
                origin: TurnOrigin::Autonomous,
            })
            .await
            .unwrap();
        events
            .send(ChatEvent::Delta {
                role: ChatRole::Assistant,
                text: "**completed**".into(),
            })
            .await
            .unwrap();
        events
            .send(ChatEvent::TurnFinished {
                ok: true,
                error: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(payload) = received.recv().await {
                assert_eq!(payload["chat_id"], 123);
                if payload["parse_mode"] == "MarkdownV2" {
                    assert_eq!(payload["message_id"], 42);
                    assert_eq!(payload["text"], "*completed*");
                    return;
                }
            }
            panic!("final reply missing");
        })
        .await
        .unwrap();
        cancel.send(true).unwrap();
        task.await.unwrap();
        server.join().unwrap();
        assert!(closed.load(std::sync::atomic::Ordering::Relaxed));
    }
    #[tokio::test]
    async fn saturated_intake_rejects_overflow_but_cancellation_still_closes() {
        let f = Fixture::new(true);
        let worker = Worker {
            tx: f.input,
            capacity: Arc::new(Semaphore::new(INTAKE_LIMIT)),
            cancel: f.cancel,
            task: f.task,
        };
        for id in 0..INTAKE_LIMIT {
            assert!(worker.submit(id as i64, "queued".into()));
        }
        assert!(!worker.submit(99, "overflow".into()));
        worker.stop().await;
        assert!(f.closed.load(std::sync::atomic::Ordering::Relaxed));
        assert!(
            f.events
                .send(ChatEvent::TurnStarted {
                    origin: TurnOrigin::Autonomous
                })
                .await
                .is_err(),
            "old generation receiver is gone before replacement acknowledgement"
        );
    }

    #[tokio::test]
    async fn backend_exit_closes_worker_and_clears_pending_acknowledgement() {
        let mut f = Fixture::new(false);
        f.submit(1, "pending").await;
        f.events
            .send(ChatEvent::SessionClosed {
                error: Some("exited".into()),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), f.task)
            .await
            .unwrap()
            .unwrap();
        tokio::task::yield_now().await;
        assert!(f.closed.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(*f.api.reactions.lock().unwrap(), [(1, true), (1, false)]);
    }
}
