use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::{
    api::{SentMessage, SlackClient},
    mrkdwn,
};

const MAX_THINKING_CHARS: usize = 3000;
const UPDATE_INTERVAL: Duration = Duration::from_secs(3);

enum Event {
    Append(String),
    Finish(oneshot::Sender<()>),
}

enum PendingEvent {
    Event(Option<Event>),
    Timer,
}

async fn next_event(rx: &mut mpsc::UnboundedReceiver<Event>, dirty: bool) -> PendingEvent {
    if dirty {
        tokio::select! {
            _ = tokio::time::sleep(UPDATE_INTERVAL) => PendingEvent::Timer,
            event = rx.recv() => PendingEvent::Event(event),
        }
    } else {
        PendingEvent::Event(rx.recv().await)
    }
}

pub struct Thinking {
    tx: mpsc::UnboundedSender<Event>,
}

impl Thinking {
    pub fn start(
        client: SlackClient,
        channel: String,
        thread_ts: Option<String>,
        delete_on_complete: bool,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut text = String::new();
            let mut posted: Option<SentMessage> = None;
            let mut dirty = false;
            loop {
                let event = next_event(&mut rx, dirty).await;
                match event {
                    PendingEvent::Timer => {
                        if let Some(message) = &posted {
                            let _ = client
                                .update_message(&channel, &message.ts, &display(&text))
                                .await;
                            dirty = false;
                        }
                    }
                    PendingEvent::Event(Some(Event::Append(delta))) => {
                        text.push_str(&delta);
                        if posted.is_none() {
                            posted = client
                                .post_message(&channel, &display(&text), thread_ts.as_deref())
                                .await
                                .ok();
                        } else {
                            dirty = true;
                        }
                    }
                    PendingEvent::Event(Some(Event::Finish(reply))) => {
                        if let Some(message) = posted {
                            if dirty {
                                let _ = client
                                    .update_message(&channel, &message.ts, &display(&text))
                                    .await;
                            }
                            if delete_on_complete {
                                let _ = client.delete_message(&channel, &message.ts).await;
                            }
                        }
                        let _ = reply.send(());
                        return;
                    }
                    PendingEvent::Event(None) => return,
                }
            }
        });
        Self { tx }
    }

    pub fn append(&self, text: String) {
        let _ = self.tx.send(Event::Append(text));
    }

    pub async fn finish(self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Event::Finish(tx)).is_ok() {
            let _ = rx.await;
        }
    }
}

fn display(text: &str) -> String {
    let mut compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() > MAX_THINKING_CHARS {
        compact = compact.chars().take(MAX_THINKING_CHARS).collect::<String>();
        compact.push('…');
    }
    compact = compact.replace(". ", ".\n").replace(" - ", "\n- ");
    format!("🧠 Thinking:\n{}", mrkdwn::render(&compact))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_caps_and_neutralizes_controls() {
        assert_eq!(
            display("hello. next - item <@U1>"),
            "🧠 Thinking:\nhello.\nnext\n- item &lt;@U1>"
        );
        let long = "x".repeat(3001);
        assert_eq!(
            display(&long).chars().count(),
            "🧠 Thinking:\n".chars().count() + 3001
        );
    }

    #[tokio::test(start_paused = true)]
    async fn trailing_update_waits_three_seconds_after_last_append() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let first_wait = tokio::spawn(async move { next_event(&mut rx, true).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!first_wait.is_finished());
        tx.send(Event::Append("second".into())).unwrap();
        assert!(matches!(
            first_wait.await.unwrap(),
            PendingEvent::Event(Some(Event::Append(_)))
        ));

        let (_tx, mut rx) = mpsc::unbounded_channel();
        let trailing_wait = tokio::spawn(async move { next_event(&mut rx, true).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!trailing_wait.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(trailing_wait.await.unwrap(), PendingEvent::Timer));
    }
}
