//! Reconnecting event stream; REST work stays outside the socket read loop.
use std::time::Duration;
#[derive(Default)]
pub struct Backoff {
    attempt: u32,
}
impl Backoff {
    pub fn next_delay(&mut self) -> Duration {
        let seconds = (1_u64 << self.attempt.min(5)).min(30);
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_secs(seconds)
    }
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}
#[derive(Default)]
pub struct ResumeState {
    connection_id: String,
    next_sequence: u64,
    disconnected_at: Option<i64>,
}
impl ResumeState {
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
    pub fn observe(&mut self, sequence: Option<u64>) {
        if let Some(sequence) = sequence {
            self.next_sequence = sequence.saturating_add(1);
        }
    }
    pub fn disconnect(&mut self, at: i64) {
        if self.disconnected_at.is_none() {
            self.disconnected_at = Some(at);
        }
    }
    pub fn hello(&mut self, connection_id: &str, sequence: Option<u64>, now: i64) -> Option<i64> {
        let gap = self
            .disconnected_at
            .take()
            .filter(|_| !self.connection_id.is_empty() && connection_id != self.connection_id)
            .map(|at| catch_up_since(at, now));
        if connection_id != self.connection_id {
            self.next_sequence = 0;
        }
        self.connection_id = connection_id.to_owned();
        self.observe(sequence);
        gap
    }
}
pub fn catch_up_since(disconnected_at: i64, now: i64) -> i64 {
    disconnected_at
        .saturating_sub(5000)
        .max(now.saturating_sub(600_000))
}
pub struct RecentPosts {
    limit: usize,
    order: std::collections::VecDeque<String>,
    ids: std::collections::HashSet<String>,
}
impl RecentPosts {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            order: Default::default(),
            ids: Default::default(),
        }
    }
    pub fn insert(&mut self, id: &str) -> bool {
        if id.is_empty() || !self.ids.insert(id.to_owned()) {
            return false;
        }
        self.order.push_back(id.to_owned());
        while self.order.len() > self.limit {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
        true
    }
}
use crate::api::{ApiError, MattermostClient, Post};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum Event {
    Posted {
        post: Box<Post>,
        channel_type: String,
        mentions: Vec<String>,
    },
    Gap {
        since: i64,
    },
}
#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    event: String,
    #[serde(default)]
    data: serde_json::Value,
    seq: Option<u64>,
}
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub async fn run(
    client: MattermostClient,
    tx: mpsc::UnboundedSender<Event>,
    cancel: CancellationToken,
) -> Result<(), ApiError> {
    let mut resume = ResumeState::default();
    let mut backoff = Backoff::default();
    let mut ping_sequence = 1_u64;
    loop {
        let request = client.websocket_request(resume.connection_id(), resume.next_sequence())?;
        let connected = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = tokio::time::timeout(Duration::from_secs(30), tokio_tungstenite::connect_async(request)) => result,
        };
        if let Ok(Ok((mut socket, _))) = connected {
            let mut last_traffic = tokio::time::Instant::now();
            let mut last_traffic_at = now_millis();
            let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
            let mut last_ping = tokio::time::Instant::now();
            let disconnected_at = loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
                        return Ok(());
                    }
                    _ = heartbeat.tick() => {
                        if last_traffic.elapsed() >= Duration::from_secs(75) { break last_traffic_at; }
                        if last_ping.elapsed() >= Duration::from_secs(30) {
                            let ping = serde_json::json!({"action":"ping","seq":ping_sequence});
                            ping_sequence = ping_sequence.saturating_add(1);
                            if !matches!(tokio::time::timeout(Duration::from_secs(5), socket.send(Message::Text(ping.to_string()))).await, Ok(Ok(()))) { break now_millis(); }
                            last_ping = tokio::time::Instant::now();
                        }
                    }
                    message = socket.next() => {
                        let Some(Ok(message)) = message else { break now_millis(); };
                        last_traffic = tokio::time::Instant::now();
                        last_traffic_at = now_millis();
                        let text = match message { Message::Text(text) => text, Message::Close(_) => break now_millis(), _ => continue };
                        let Ok(envelope) = serde_json::from_str::<Envelope>(&text) else { continue; };
                        if envelope.event == "hello" {
                            let id = envelope.data["connection_id"].as_str().unwrap_or_default();
                            if id.is_empty() { continue; }
                            if let Some(since) = resume.hello(id, envelope.seq, now_millis()) { if tx.send(Event::Gap { since }).is_err() { return Ok(()); } }
                            backoff.reset();
                            continue;
                        }
                        resume.observe(envelope.seq);
                        if envelope.event != "posted" { continue; }
                        let Some(raw_post) = envelope.data["post"].as_str() else { continue; };
                        let Ok(post) = serde_json::from_str::<Post>(raw_post) else { continue; };
                        let mentions = envelope.data["mentions"].as_str().and_then(|raw| serde_json::from_str(raw).ok()).unwrap_or_default();
                        let channel_type = envelope.data["channel_type"].as_str().unwrap_or_default().to_owned();
                        if tx.send(Event::Posted { post: Box::new(post), channel_type, mentions }).is_err() { return Ok(()); }
                    }
                }
            };
            resume.disconnect(disconnected_at);
            let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
        } else {
            resume.disconnect(now_millis());
        }
        let delay = backoff.next_delay();
        tracing::warn!(
            seconds = delay.as_secs(),
            "mattermost socket disconnected; retrying"
        );
        tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(delay) => {} }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reconnect_delays_grow_then_stay_at_thirty_seconds() {
        let mut backoff = Backoff::default();
        let delays: Vec<_> = (0..7).map(|_| backoff.next_delay().as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30]);
        backoff.reset();
        assert_eq!(backoff.next_delay().as_secs(), 1);
    }
    #[test]
    fn a_new_connection_requests_catch_up_but_a_resumed_connection_does_not() {
        let mut resume = ResumeState::default();
        assert_eq!(resume.hello("connectionone", Some(3), 1_000_000), None);
        resume.observe(Some(7));
        resume.disconnect(1_010_000);
        assert_eq!(resume.connection_id(), "connectionone");
        assert_eq!(resume.next_sequence(), 8);
        assert_eq!(resume.hello("connectionone", Some(8), 1_020_000), None);
        resume.disconnect(1_030_000);
        assert_eq!(
            resume.hello("connectiontwo", Some(0), 1_040_000),
            Some(1_025_000)
        );
        assert_eq!(resume.next_sequence(), 1);
    }

    #[test]
    fn catch_up_is_bounded_for_short_and_long_outages_and_replays_are_deduplicated() {
        assert_eq!(catch_up_since(1_000_000, 1_010_000), 995_000);
        assert_eq!(catch_up_since(1_000_000, 3_000_000), 2_400_000);
        let mut recent = RecentPosts::new(2);
        assert!(recent.insert("first000000000000000000000"));
        assert!(!recent.insert("first000000000000000000000"));
        assert!(recent.insert("second00000000000000000000"));
        assert!(recent.insert("third000000000000000000000"));
        assert!(recent.insert("first000000000000000000000"));
    }

    struct InspectHandshake;
    impl tokio_tungstenite::tungstenite::handshake::server::Callback for InspectHandshake {
        fn on_request(
            self,
            request: &tokio_tungstenite::tungstenite::handshake::server::Request,
            response: tokio_tungstenite::tungstenite::handshake::server::Response,
        ) -> Result<
            tokio_tungstenite::tungstenite::handshake::server::Response,
            tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
        > {
            assert_eq!(request.uri().path(), "/api/v4/websocket");
            assert_eq!(request.headers()["authorization"], "Bearer invented-token");
            Ok(response)
        }
    }
    #[tokio::test]
    async fn socket_routes_new_posts_only_and_shutdown_closes_the_connection() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::{net::TcpListener, sync::mpsc};
        use tokio_tungstenite::tungstenite::Message;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = crate::api::MattermostClient::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            crate::config::SecretToken::new("invented-token".into()),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(stream, InspectHandshake)
                .await
                .unwrap();
            for message in [
                serde_json::json!({"event":"hello","data":{"connection_id":"one00000000000000000000000"},"seq":0}),
                serde_json::json!({"event":"post_edited","data":{"post":"{\"id\":\"edited00000000000000000000\"}"},"seq":1}),
                serde_json::json!({"event":"posted","data":{"post":"{\"id\":\"source00000000000000000000\",\"message\":\"hello\"}","channel_type":"P","mentions":"[\"helper00000000000000000000\"]"},"seq":2}),
            ] {
                socket
                    .send(Message::Text(message.to_string()))
                    .await
                    .unwrap();
            }
            while let Some(message) = socket.next().await {
                if matches!(message.unwrap(), Message::Close(_)) {
                    return;
                }
            }
        });
        let cancel = tokio_util::sync::CancellationToken::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run(client, tx, cancel.clone()));
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let Event::Posted {
            post,
            channel_type,
            mentions,
        } = event
        else {
            panic!("expected post")
        };
        assert_eq!(post.id, "source00000000000000000000");
        assert_eq!(channel_type, "P");
        assert_eq!(mentions, ["helper00000000000000000000"]);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }
}
