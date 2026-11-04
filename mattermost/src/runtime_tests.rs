use super::*;
use dar_extension_sdk::chat::{
    BoxFuture, ChatBackend, ChatEvent, ChatRole, ChatSession, ChatSessionParams,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

struct ScriptBackend {
    events: Vec<ChatEvent>,
    prompts: mpsc::UnboundedSender<String>,
    artifact: Option<dar_extension_sdk::chat::ArtifactReady>,
    hang_abort: bool,
    hang_send: bool,
    abort_started: Arc<AtomicBool>,
}
struct ScriptSession {
    events: Vec<ChatEvent>,
    tx: mpsc::Sender<ChatEvent>,
    prompts: mpsc::UnboundedSender<String>,
    hang_abort: bool,
    hang_send: bool,
    abort_started: Arc<AtomicBool>,
}
impl ChatBackend for ScriptBackend {
    fn open<'a>(
        &'a self,
        params: ChatSessionParams,
        tx: mpsc::Sender<ChatEvent>,
    ) -> BoxFuture<'a, Result<Box<dyn ChatSession>>> {
        Box::pin(async move {
            if let (Some(artifact), Some(sender)) = (&self.artifact, params.artifact_ready) {
                sender.send(artifact.clone()).await?;
            }
            Ok(Box::new(ScriptSession {
                events: self.events.clone(),
                tx,
                prompts: self.prompts.clone(),
                hang_abort: self.hang_abort,
                hang_send: self.hang_send,
                abort_started: self.abort_started.clone(),
            }) as Box<dyn ChatSession>)
        })
    }
}
impl ChatSession for ScriptSession {
    fn send_turn(&mut self, text: String) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let _ = self.prompts.send(text);
            if self.hang_send {
                std::future::pending::<()>().await;
            }
            for event in self.events.clone() {
                self.tx.send(event).await?;
            }
            Ok(())
        })
    }
    fn abort(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.abort_started.store(true, Ordering::Relaxed);
            if self.hang_abort {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct Harness {
    client: MattermostClient,
    inbound: mpsc::UnboundedSender<Value>,
    requests: mpsc::UnboundedReceiver<(String, String, Value)>,
    cancel: CancellationToken,
    server: tokio::task::JoinHandle<()>,
    history: Arc<std::sync::Mutex<Value>>,
    fail_users: Arc<AtomicBool>,
    fail_ack: Arc<AtomicBool>,
    stall_history: Arc<AtomicBool>,
}
impl Harness {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let client = MattermostClient::new(
            &base,
            crate::config::SecretToken::new("invented-token".into()),
        )
        .unwrap();
        let (inbound, input) = mpsc::unbounded_channel::<Value>();
        let input = Arc::new(tokio::sync::Mutex::new(input));
        let (record, requests) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let history = Arc::new(std::sync::Mutex::new(json!({"order":[],"posts":{}})));
        let history_response = history.clone();
        let fail_users = Arc::new(AtomicBool::new(false));
        let user_failure = fail_users.clone();
        let fail_ack = Arc::new(AtomicBool::new(false));
        let ack_failure = fail_ack.clone();
        let stall_history = Arc::new(AtomicBool::new(false));
        let history_stall = stall_history.clone();
        let stopping = cancel.clone();
        let connections = Arc::new(AtomicU64::new(0));
        let post_ids = Arc::new(AtomicU64::new(0));
        let server = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopping.cancelled() => break,
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let record = record.clone(); let input = input.clone(); let cancel = stopping.clone();let history=history_response.clone();let connections=connections.clone();let post_ids=post_ids.clone();let fail_users=user_failure.clone();let fail_ack=ack_failure.clone();let stall_history=history_stall.clone();
                        tasks.spawn(async move {
                            let mut header = [0;8192];
                            loop {
                                let n = socket.peek(&mut header).await.unwrap();
                                if n == 0 {return;}
                                if header[..n].windows(4).any(|w|w==b"\r\n\r\n") {break;}
                                tokio::task::yield_now().await;
                            }
                            if String::from_utf8_lossy(&header).contains("/websocket") {
                                let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                                let connection=connections.fetch_add(1,Ordering::Relaxed);
                                ws.send(Message::Text(json!({"event":"hello","seq":0,"data":{"connection_id":format!("w{connection:025}")}}).to_string())).await.unwrap();
                                let mut input = input.lock().await;
                                let mut sequence = 1;
                                loop { tokio::select! {
                                    _ = cancel.cancelled() => {let _=ws.close(None).await;break;},
                                    event = input.recv() => if let Some(mut event) = event {if event["event"]=="_disconnect"{let _=ws.close(None).await;break;}event["seq"]=json!(sequence);sequence+=1; if ws.send(Message::Text(event.to_string())).await.is_err(){break;}},
                                    received = ws.next() => if !matches!(received,Some(Ok(_))) {break;},
                                }}
                                return;
                            }
                            let mut bytes=Vec::new();
                            let end=loop {let mut b=[0;8192];let n=socket.read(&mut b).await.unwrap();if n==0{return;}bytes.extend_from_slice(&b[..n]);if let Some(n)=bytes.windows(4).position(|w|w==b"\r\n\r\n"){break n+4;}};
                            let header=String::from_utf8_lossy(&bytes[..end]).into_owned();
                            let size=header.lines().find_map(|l| l.split_once(':').filter(|(n,_)|n.eq_ignore_ascii_case("content-length")).and_then(|(_,v)|v.trim().parse::<usize>().ok())).unwrap_or(0);
                            while bytes.len()-end<size {let mut b=[0;8192];let n=socket.read(&mut b).await.unwrap();if n==0{return;}bytes.extend_from_slice(&b[..n]);}
                            let mut line=header.lines().next().unwrap().split_whitespace();
                            let method=line.next().unwrap().to_owned(); let path=line.next().unwrap().to_owned();
                            let body:Value=serde_json::from_slice(&bytes[end..end+size]).unwrap_or(Value::Null);
                            let response=if path=="/api/v4/users/me" {json!({"id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","username":"helper","is_bot":true})}
                                else if path=="/api/v4/files/iiiiiiiiiiiiiiiiiiiiiiiiii/info"{json!({"id":"iiiiiiiiiiiiiiiiiiiiiiiiii","name":"sample.txt","size":18,"mime_type":"text/plain"})}
                                else if path=="/api/v4/files/iiiiiiiiiiiiiiiiiiiiiiiiii"{json!({"sample":"content"})}
                                else if path.starts_with("/api/v4/files?channel_id="){json!({"file_infos":[{"id":"ffffffffffffffffffffffffff"}]})}
                                else if path=="/api/v4/users/ids"&&fail_users.load(Ordering::Relaxed){json!({})}
                                else if path=="/api/v4/users/ids" {json!([{"id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","username":"sage","first_name":"Sage","last_name":"Green"}])}
                                else if path=="/api/v4/posts" {json!({"id":format!("r{:025}",post_ids.fetch_add(1,Ordering::Relaxed)),"message":body["message"],"channel_id":body["channel_id"]})}
                                else if path.contains("/posts?")||path.ends_with("/thread"){history.lock().unwrap().clone()}
                                else if path.contains("/channels/") && !path.contains("/posts") {json!({"id":"cccccccccccccccccccccccccc","type":"D","name":"bbbbbbbbbbbbbbbbbbbbbbbbbb__uuuuuuuuuuuuuuuuuuuuuuuuuu"})}
                                else {json!({})};
                            let _=record.send((method,path,body));
                            if header.contains("/thread ")||header.contains("/posts?"){while stall_history.load(Ordering::Relaxed){tokio::time::sleep(Duration::from_millis(5)).await;}}
                            let status=if header.lines().next().is_some_and(|line|line.starts_with("POST /api/v4/reactions ")) && fail_ack.load(Ordering::Relaxed) {403} else {200};
                            let response=response.to_string();
                            let _=socket.write_all(format!("HTTP/1.1 {status} test\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{response}",response.len()).as_bytes()).await;
                        });
                    }
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        });
        Self {
            client,
            inbound,
            requests,
            cancel,
            server,
            history,
            fail_users,
            fail_ack,
            stall_history,
        }
    }
    fn post(&self, id: &str, text: &str, root: &str) {
        self.post_kind(id, text, root, "D");
    }
    fn post_kind(&self, id: &str, text: &str, root: &str, kind: &str) {
        self.inbound.send(json!({"event":"posted","data":{"channel_type":kind,"mentions":"[]","post":json!({"id":id,"user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","root_id":root,"message":text,"props":{},"file_ids":[]}).to_string()}})).unwrap();
    }
    async fn posted(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let (method, path, body) = self.requests.recv().await.unwrap();
                if method == "POST" && path == "/api/v4/posts" {
                    return body;
                }
            }
        })
        .await
        .expect("expected visible reply")
    }
    async fn stop(self) {
        self.cancel.cancel();
        self.server.await.unwrap();
    }
}

fn context(
    events: Vec<ChatEvent>,
) -> (
    StartCtx,
    tokio::sync::watch::Sender<bool>,
    mpsc::UnboundedReceiver<String>,
) {
    context_with_artifact(events, None)
}
fn context_with_artifact(
    events: Vec<ChatEvent>,
    artifact: Option<(
        Arc<dar_extension_sdk::artifacts::ArtifactStore>,
        dar_extension_sdk::chat::ArtifactReady,
    )>,
) -> (
    StartCtx,
    tokio::sync::watch::Sender<bool>,
    mpsc::UnboundedReceiver<String>,
) {
    let (ctx, stop, prompts, _) = context_with_options(events, artifact, false, false);
    (ctx, stop, prompts)
}
fn context_with_options(
    events: Vec<ChatEvent>,
    artifact: Option<(
        Arc<dar_extension_sdk::artifacts::ArtifactStore>,
        dar_extension_sdk::chat::ArtifactReady,
    )>,
    hang_abort: bool,
    hang_send: bool,
) -> (
    StartCtx,
    tokio::sync::watch::Sender<bool>,
    mpsc::UnboundedReceiver<String>,
    Arc<AtomicBool>,
) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/runtime-tests")
        .join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&root).unwrap();
    let paths = host_api::HostPaths::new(&root).unwrap();
    let (stop, rx) = tokio::sync::watch::channel(false);
    let (prompts, seen) = mpsc::unbounded_channel();
    let abort_started = Arc::new(AtomicBool::new(false));
    let mut register = host_api::RegisterCtx {
        bus: host_api::EventBus::new(),
        http: host_api::HttpRegistry::disabled(),
        foreground: host_api::ForegroundRegistry::default(),
        services: host_api::ServiceRegistry::default(),
        paths: paths.clone(),
        config: host_api::ConfigStore::default(),
        shutdown: host_api::ShutdownToken::new(rx.clone()),
    };
    if let Some((store, _)) = &artifact {
        register
            .services
            .service::<dar_extension_sdk::artifacts::ArtifactStore>("artifact-store", store.clone())
            .unwrap();
    }
    register
        .services
        .service::<dyn ChatBackend>(
            "scripted",
            Arc::new(ScriptBackend {
                events,
                prompts,
                artifact: artifact.map(|(_, ready)| ready),
                hang_abort,
                hang_send,
                abort_started: abort_started.clone(),
            }),
        )
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
        stop,
        seen,
        abort_started,
    )
}

#[tokio::test]
async fn dm_reaches_agent_and_receives_a_flat_reply() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "Hello Sage".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        fetch_history: false,
        ack_reaction: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "hello", "");
    let reply = wire.posted().await;
    assert_eq!(reply["message"], "Hello Sage");
    assert!(reply["root_id"].is_null() || reply["root_id"] == "");
    let prompt = prompts.recv().await.unwrap();
    assert!(prompt.contains("From Sage Green (@sage):\nhello"));
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn reset_and_stop_confirm_even_without_an_active_turn() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![ChatEvent::TurnFinished {
        ok: true,
        error: None,
    }]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        fetch_history: false,
        ack_reaction: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("nnnnnnnnnnnnnnnnnnnnnnnnnn", "!new", "");
    assert_eq!(
        wire.posted().await["message"],
        "Context cleared, new session started."
    );
    wire.post("zzzzzzzzzzzzzzzzzzzzzzzzzz", "!stop", "");
    assert_eq!(wire.posted().await["message"], "Stopped current response.");
    assert!(
        prompts.try_recv().is_err(),
        "commands never reach the agent"
    );
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn silent_turn_removes_ack_and_posts_nothing() {
    let mut wire = Harness::start().await;
    let (ctx, stop, _) = context(vec![
        ChatEvent::Silent {
            reason: None,
            text: "NO_REPLY".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "keep quiet", "");
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut acknowledged = false;
        loop {
            let (method, path, body) = wire.requests.recv().await.unwrap();
            assert!(
                !(method == "POST" && path == "/api/v4/posts"),
                "silent turn posted: {body}"
            );
            if path == "/api/v4/reactions" {
                acknowledged = true;
                assert_eq!(body["emoji_name"], "eyes");
            }
            if method == "DELETE" && path == "/api/v4/users/bbbbbbbbbbbbbbbbbbbbbbbbbb/posts/ssssssssssssssssssssssssss/reactions/eyes" {
                assert!(acknowledged);
                break;
            }
        }
    })
    .await
    .expect("silent acknowledgement cleanup");
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn channel_mention_and_unmentioned_thread_followup_share_history() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "answer".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        fetch_history: false,
        ack_reaction: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post_kind(
        "ssssssssssssssssssssssssss",
        "@helper first question",
        "",
        "O",
    );
    assert_eq!(wire.posted().await["root_id"], "ssssssssssssssssssssssssss");
    prompts.recv().await.unwrap();
    wire.post_kind(
        "ffffffffffffffffffffffffff",
        "second question",
        "ssssssssssssssssssssssssss",
        "O",
    );
    assert_eq!(wire.posted().await["root_id"], "ssssssssssssssssssssssssss");
    let prompt = prompts.recv().await.unwrap();
    assert!(
        prompt.contains("first question"),
        "thread history missing: {prompt}"
    );
    assert!(prompt.contains("Untrusted Mattermost conversation context."));
    assert!(prompt.contains("Current Mattermost message:"));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn restart_backfill_recovers_root_context_without_thread_messages() {
    let mut wire = Harness::start().await;
    *wire.history.lock().unwrap() = json!({"order":["eeeeeeeeeeeeeeeeeeeeeeeeee","aaaaaaaaaaaaaaaaaaaaaaaaaa"],"posts":{"aaaaaaaaaaaaaaaaaaaaaaaaaa":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"earlier root point"},"eeeeeeeeeeeeeeeeeeeeeeeeee":{"id":"eeeeeeeeeeeeeeeeeeeeeeeeee","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","root_id":"xxxxxxxxxxxxxxxxxxxxxxxxxx","message":"different thread secret"}}});
    let (ctx, stop, mut prompts) = context(vec![ChatEvent::TurnFinished {
        ok: true,
        error: None,
    }]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "what was the point?", "");
    wire.posted().await;
    let prompt = prompts.recv().await.unwrap();
    assert!(
        prompt.contains("earlier root point"),
        "missing restart context: {prompt}"
    );
    assert!(!prompt.contains("different thread secret"));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn typing_keeps_refreshing_but_does_not_extend_no_output_deadline() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "please work", "");
    tokio::time::timeout(Duration::from_secs(3), prompts.recv())
        .await
        .unwrap()
        .unwrap();
    while wire.requests.try_recv().is_ok() {}
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(5)).await;
    loop {
        let (method, path, body) = wire.requests.recv().await.unwrap();
        assert_ne!(
            path, "/api/v4/posts",
            "deadline output arrived before typing: {body}"
        );
        if path == "/api/v4/users/bbbbbbbbbbbbbbbbbbbbbbbbbb/typing" {
            assert_eq!(method, "POST");
            break;
        }
    }
    tokio::time::advance(Duration::from_secs(56)).await;
    assert_eq!(
        wire.posted().await["message"],
        "Sorry, I couldn't complete that request. Please try again."
    );
    tokio::time::resume();
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn attachment_only_post_reaches_agent_as_local_untrusted_metadata() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![ChatEvent::TurnFinished {
        ok: true,
        error: None,
    }]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":"ssssssssssssssssssssssssss","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","file_ids":["iiiiiiiiiiiiiiiiiiiiiiiiii"]}).to_string()}})).unwrap();
    wire.posted().await;
    let prompt = prompts.recv().await.unwrap();
    assert!(
        prompt.contains("sample.txt"),
        "attachment metadata missing: {prompt}"
    );
    assert!(prompt.contains("untrusted Mattermost attachment"));
    assert!(prompt.contains("data/uploads/"));
    assert!(!prompt.contains("invented-token"));
    assert!(!prompt.contains("http://"));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn bot_loop_guard_survives_fresh_sessions_and_human_input_resets_it() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![ChatEvent::TurnFinished {
        ok: true,
        error: None,
    }]);
    std::fs::write(
        ctx.paths.root().join("agent.yaml"),
        "agent_loop: {max_agent_turns: 2, max_hops: 5}\n",
    )
    .unwrap();
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: true,
        fetch_history: false,
        allow_bots: crate::config::AllowBots::All(true),
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    for id in ["llllllllllllllllllllllllll", "mmmmmmmmmmmmmmmmmmmmmmmmmm"] {
        wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":id,"user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"@helper hi","props":{"from_bot":true}}).to_string()}})).unwrap();
        wire.posted().await;
        prompts.recv().await.unwrap();
    }
    wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":"pppppppppppppppppppppppppp","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"@helper hi","props":{"from_bot":true}}).to_string()}})).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (method, path, body) = wire.requests.recv().await.unwrap();
            assert!(
                !(method == "POST" && path == "/api/v4/posts"),
                "blocked bot turn posted: {body}"
            );
            if method == "DELETE" && path.contains("/posts/pppppppppppppppppppppppppp/reactions/") {
                break;
            }
        }
    })
    .await
    .expect("loop guard removes acknowledgement");
    assert!(prompts.try_recv().is_err(), "blocked turn reached backend");
    wire.post("hhhhhhhhhhhhhhhhhhhhhhhhhh", "hello", "");
    wire.posted().await;
    prompts.recv().await.unwrap();
    wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":"qqqqqqqqqqqqqqqqqqqqqqqqqq","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"@helper hi","props":{"from_bot":true}}).to_string()}})).unwrap();
    wire.posted().await;
    prompts.recv().await.unwrap();
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn new_connection_catches_up_seen_dm_and_deduplicates_replay() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![ChatEvent::TurnFinished {
        ok: true,
        error: None,
    }]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        channels: Some(Default::default()),
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("dddddddddddddddddddddddddd", "first", "");
    wire.posted().await;
    prompts.recv().await.unwrap();
    *wire.history.lock().unwrap() = json!({"order":["kkkkkkkkkkkkkkkkkkkkkkkkkk","dddddddddddddddddddddddddd"],"posts":{"kkkkkkkkkkkkkkkkkkkkkkkkkk":{"id":"kkkkkkkkkkkkkkkkkkkkkkkkkk","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"during outage","create_at":crate::ws::now_millis()},"dddddddddddddddddddddddddd":{"id":"dddddddddddddddddddddddddd","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"first"}}});
    wire.inbound.send(json!({"event":"_disconnect"})).unwrap();
    wire.posted().await;
    assert!(prompts.recv().await.unwrap().contains("during outage"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), prompts.recv())
            .await
            .is_err(),
        "replayed post handled twice"
    );
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn late_silence_removes_retained_thinking_live_reply_and_deferred_expiry_notice() {
    let mut wire = Harness::start().await;
    let (ctx, stop, _) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Thinking,
            text: "working".into(),
        },
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "draft".into(),
        },
        ChatEvent::Silent {
            reason: None,
            text: "NO_REPLY".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    crate::session::prepare_activity(
        &data,
        &crate::session::SessionKey::dm("cccccccccccccccccccccccccc"),
        1,
        crate::session::now() - 61,
    )
    .unwrap();
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        show_thinking: true,
        delete_thinking_on_complete: false,
        fetch_history: false,
        sessions: crate::config::SessionsConfig { idle_minutes: 1 },
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "consider quietly", "");
    let mut created = Vec::new();
    let mut deleted = std::collections::HashSet::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (method, path, body) = wire.requests.recv().await.unwrap();
            if method == "POST" && path == "/api/v4/posts" {
                created.push(body["message"].as_str().unwrap().to_owned());
            }
            if method == "DELETE" && path.starts_with("/api/v4/posts/") {
                deleted.insert(path.clone());
            }
            if method == "DELETE" && path.contains("/reactions/") {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        created,
        vec![
            "Previous session expired; starting fresh.".to_owned(),
            "🧠 Thinking:\nworking".into(),
            "draft".into()
        ]
    );
    assert_eq!(deleted.len(), 3, "late silence left provisional output");
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn published_artifact_is_delivered_after_text_in_the_same_thread() {
    use dar_extension_sdk::{
        artifacts::{ArtifactMetadataInput, ArtifactStore, ExportRoot},
        chat::ArtifactReady,
    };
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/runtime-artifact-fixture");
    std::fs::create_dir_all(fixture.join("exports")).unwrap();
    std::fs::write(fixture.join("exports/published.txt"), "report").unwrap();
    let store = Arc::new(ArtifactStore::open(fixture.join("vault"), 1024).unwrap());
    let metadata = store
        .stage_from_export_root(
            &ExportRoot::open(fixture.join("exports")).unwrap(),
            "published.txt",
            ArtifactMetadataInput {
                filename: "published.txt".into(),
                media_type: Some("text/plain".into()),
                caption: None,
            },
        )
        .unwrap();
    let ready = ArtifactReady {
        id: metadata.id,
        uri: format!("dar-artifact://{}", metadata.id),
        name: metadata.filename.clone(),
        mime_type: metadata.media_type.clone(),
        bytes: metadata.bytes,
        sha256: metadata.sha256_hex(),
        caption: None,
    };
    let mut wire = Harness::start().await;
    let (ctx, stop, _) = context_with_artifact(
        vec![
            ChatEvent::Delta {
                role: ChatRole::Assistant,
                text: "done".into(),
            },
            ChatEvent::TurnFinished {
                ok: true,
                error: None,
            },
        ],
        Some((store, ready)),
    );
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post(
        "ssssssssssssssssssssssssss",
        "publish a file",
        "tttttttttttttttttttttttttt",
    );
    let text = wire.posted().await;
    assert_eq!(text["message"], "done");
    assert_eq!(text["root_id"], "tttttttttttttttttttttttttt");
    let file = wire.posted().await;
    assert_eq!(file["root_id"], "tttttttttttttttttttttttttt");
    assert_eq!(file["file_ids"], json!(["ffffffffffffffffffffffffff"]));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn failed_identity_lookup_cannot_post_into_disallowed_or_unaddressed_conversations() {
    for (kind, dm_enabled, restricted, text) in [
        ("O", true, true, "@helper hi"),
        ("D", false, false, "hi"),
        ("O", true, false, "ordinary conversation"),
    ] {
        let mut wire = Harness::start().await;
        wire.fail_users.store(true, Ordering::Relaxed);
        let (ctx, stop, mut prompts) = context(vec![]);
        let data = ctx.paths.root().join("mattermost");
        let mut cfg = MattermostConfig {
            backend: Some("scripted".into()),
            ack_reaction: false,
            fetch_history: false,
            ..Default::default()
        };
        cfg.dm.enabled = dm_enabled;
        if restricted {
            cfg.channels = Some(Default::default());
        }
        let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
        wire.post_kind("ssssssssssssssssssssssssss", text, "", kind);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), wire.posted())
                .await
                .is_err(),
            "unauthorized error reply"
        );
        assert!(prompts.try_recv().is_err());
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        wire.stop().await;
    }
}

#[tokio::test]
async fn thread_engagement_is_visible_while_the_first_turn_is_backfilling() {
    let mut wire = Harness::start().await;
    wire.stall_history.store(true, Ordering::Relaxed);
    let (ctx, stop, mut prompts) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "answer".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post_kind("ssssssssssssssssssssssssss", "@helper first", "", "O");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (_, path, _) = wire.requests.recv().await.unwrap();
            if path.ends_with("/thread") {
                break;
            }
        }
    })
    .await
    .unwrap();
    wire.post_kind(
        "ffffffffffffffffffffffffff",
        "followup without mention",
        "ssssssssssssssssssssssssss",
        "O",
    );
    wire.posted().await;
    assert!(prompts
        .recv()
        .await
        .unwrap()
        .contains("followup without mention"));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn a_hung_abort_is_bounded_and_other_conversations_still_progress() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts, abort_started) = context_with_options(vec![], None, true, false);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "slow agent", "");
    tokio::time::timeout(Duration::from_secs(2), prompts.recv())
        .await
        .unwrap()
        .unwrap();
    wire.post("zzzzzzzzzzzzzzzzzzzzzzzzzz", "!stop", "");
    wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":"jjjjjjjjjjjjjjjjjjjjjjjjjj","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"oooooooooooooooooooooooooo","message":"!ping"}).to_string()}})).unwrap();
    let reply = wire.posted().await;
    assert_eq!(reply["channel_id"], "oooooooooooooooooooooooooo");
    assert_eq!(reply["message"], "pong");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !abort_started.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stop should reach the backend abort interface");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(wire.posted().await["message"], "Stopped current response.");
    tokio::time::resume();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn delayed_catchup_cannot_replace_a_newer_accepted_live_turn() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "answer".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        channels: Some(Default::default()),
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("dddddddddddddddddddddddddd", "first", "");
    wire.posted().await;
    prompts.recv().await.unwrap();
    *wire.history.lock().unwrap() = json!({"order":["kkkkkkkkkkkkkkkkkkkkkkkkkk"],"posts":{"kkkkkkkkkkkkkkkkkkkkkkkkkk":{"id":"kkkkkkkkkkkkkkkkkkkkkkkkkk","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"older during outage","create_at":crate::ws::now_millis()}}});
    wire.stall_history.store(true, Ordering::Relaxed);
    wire.inbound.send(json!({"event":"_disconnect"})).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, path, _) = wire.requests.recv().await.unwrap();
            if path.contains("/posts?") {
                break;
            }
        }
    })
    .await
    .unwrap();
    wire.inbound.send(json!({"event":"posted","data":{"channel_type":"D","post":json!({"id":"vvvvvvvvvvvvvvvvvvvvvvvvvv","user_id":"uuuuuuuuuuuuuuuuuuuuuuuuuu","channel_id":"cccccccccccccccccccccccccc","message":"newest live question","create_at":crate::ws::now_millis()+1000}).to_string()}})).unwrap();
    wire.posted().await;
    assert!(prompts
        .recv()
        .await
        .unwrap()
        .contains("newest live question"));
    wire.stall_history.store(false, Ordering::Relaxed);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), prompts.recv())
            .await
            .is_err(),
        "stale catchup displaced newer work"
    );
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn a_turn_that_never_accepts_input_times_out_after_sixty_seconds_with_visible_error() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts, abort_started) = context_with_options(vec![], None, false, true);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "please accept this turn", "");
    let prompt = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(prompt.contains("please accept this turn"));
    assert!(
        tokio::time::timeout(Duration::from_millis(40), wire.posted())
            .await
            .is_err()
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    assert_eq!(
        wire.posted().await["message"],
        "Sorry, I couldn't complete that request. Please try again."
    );
    assert!(
        abort_started.load(Ordering::Relaxed),
        "timed-out turn should be aborted"
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    wire.stop().await;
}

#[tokio::test]
async fn shutdown_cancels_and_awaits_an_active_turn_without_posting_an_error() {
    let mut wire = Harness::start().await;
    let (ctx, stop, mut prompts, abort_started) = context_with_options(vec![], None, false, false);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        ack_reaction: false,
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post("ssssssssssssssssssssssssss", "wait for a reply", "");
    tokio::time::timeout(Duration::from_secs(2), prompts.recv())
        .await
        .unwrap()
        .unwrap();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        abort_started.load(Ordering::Relaxed),
        "shutdown should abort the active session"
    );
    while let Ok((method, path, body)) = wire.requests.try_recv() {
        assert!(
            !(method == "POST" && path == "/api/v4/posts"),
            "cancelled turn posted visible output: {body}"
        );
    }
    wire.stop().await;
}

#[tokio::test]
async fn failed_acknowledgement_does_not_prevent_the_agent_from_answering() {
    let mut wire = Harness::start().await;
    wire.fail_ack.store(true, Ordering::Relaxed);
    let (ctx, stop, mut prompts) = context(vec![
        ChatEvent::Delta {
            role: ChatRole::Assistant,
            text: "The answer still arrives.".into(),
        },
        ChatEvent::TurnFinished {
            ok: true,
            error: None,
        },
    ]);
    let data = ctx.paths.root().join("mattermost");
    let cfg = MattermostConfig {
        backend: Some("scripted".into()),
        fetch_history: false,
        ..Default::default()
    };
    let task = tokio::spawn(run(ctx, cfg, wire.client.clone(), data));
    wire.post(
        "ssssssssssssssssssssssssss",
        "answer despite the failed reaction",
        "",
    );
    let mut saw_failed_ack = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (method, path, body) = wire.requests.recv().await.unwrap();
            if method == "POST" && path == "/api/v4/reactions" {
                saw_failed_ack = true;
                assert_eq!(body["emoji_name"], "eyes");
            }
            if method == "POST" && path == "/api/v4/posts" {
                assert_eq!(body["message"], "The answer still arrives.");
                break;
            }
        }
    })
    .await
    .expect("a failed ack must not stall the answer");
    assert!(
        saw_failed_ack,
        "test must exercise the rejected reaction request"
    );
    assert!(tokio::time::timeout(Duration::from_secs(2), prompts.recv())
        .await
        .unwrap()
        .unwrap()
        .contains("answer despite the failed reaction"));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    wire.stop().await;
}
