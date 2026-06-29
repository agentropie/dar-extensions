//! Acknowledgement guard for an in-flight Telegram turn.
//!
//! The instant a message is picked up the bot must say "I'm working on it":
//! a `👀` reaction is added to the user's message and the "typing…" indicator
//! is shown. Telegram expires the typing action after ~5s, so it is refreshed
//! on a timer for the whole turn. When the turn ends — by success, error, or
//! panic — both signals must clear: the reaction is removed and the refresh
//! stops. That "always clears" guarantee is the whole point of this module, so
//! cleanup is wired to `Drop` and runs on every exit path.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinHandle;

/// The `👀` reaction used as the pure "working" marker.
pub const WORKING_EMOJI: &str = "👀";
/// Telegram's `typing` chat action.
pub const TYPING_ACTION: &str = "typing";
/// How often to re-send the typing action. Telegram expires it after ~5s, so a
/// shorter cadence keeps the indicator continuously visible on a long turn.
const TYPING_REFRESH: Duration = Duration::from_secs(4);

/// The Bot API surface the guard needs. Abstracted as a trait so tests can
/// inject a fake sink and assert which calls were emitted and when, without
/// hitting the network.
#[async_trait]
pub trait BotApi: Send + Sync + 'static {
    /// Set (or, with `emoji = None`, clear) the reaction on a message.
    async fn set_reaction(&self, chat_id: i64, message_id: i64, emoji: Option<&str>);
    /// Send a chat action such as `typing`.
    async fn send_chat_action(&self, chat_id: i64, action: &str);
}

/// Brackets one turn: adds `👀` + typing on start, keeps typing alive while
/// alive, and clears `👀` + stops typing on drop.
pub struct AckGuard {
    api: Arc<dyn BotApi>,
    chat_id: i64,
    message_id: i64,
    refresh: Option<JoinHandle<()>>,
    /// Set once the reaction has been cleared, so `finish` and `Drop` never
    /// double-clear.
    cleared: bool,
}

impl AckGuard {
    /// Add the `👀` reaction, send the first typing action, and start the
    /// background refresh that keeps typing alive for the rest of the turn.
    pub async fn start(api: Arc<dyn BotApi>, chat_id: i64, message_id: i64) -> Self {
        api.set_reaction(chat_id, message_id, Some(WORKING_EMOJI))
            .await;
        api.send_chat_action(chat_id, TYPING_ACTION).await;

        let refresh_api = Arc::clone(&api);
        let refresh = tokio::spawn(async move {
            // The first typing action was already sent above; sleep, then keep
            // re-sending so the indicator never lapses on a long turn.
            loop {
                tokio::time::sleep(TYPING_REFRESH).await;
                refresh_api.send_chat_action(chat_id, TYPING_ACTION).await;
            }
        });

        Self {
            api,
            chat_id,
            message_id,
            refresh: Some(refresh),
            cleared: false,
        }
    }

    /// Stop typing and clear the `👀` reaction, awaiting the clear so it is
    /// guaranteed delivered before returning. This is the preferred exit on
    /// the normal path (after the reply is sent): it does not rely on the
    /// runtime still being alive at drop time. Idempotent — `Drop` becomes a
    /// no-op once this has run.
    pub async fn finish(mut self) {
        self.stop_typing();
        self.api
            .set_reaction(self.chat_id, self.message_id, None)
            .await;
        self.cleared = true;
    }

    fn stop_typing(&mut self) {
        if let Some(handle) = self.refresh.take() {
            handle.abort();
        }
    }
}

impl Drop for AckGuard {
    fn drop(&mut self) {
        // Stop refreshing typing first so no further typing actions are sent.
        self.stop_typing();
        // If `finish` already cleared the reaction, there is nothing to do.
        if self.cleared {
            return;
        }
        // Safety net for the error / panic-unwind paths, where the guard is
        // dropped without `finish`. Drop can't await, so dispatch the clear
        // onto the runtime if one is still available; if the runtime is gone
        // there is nothing recoverable to do here (see user story 7).
        let api = Arc::clone(&self.api);
        let chat_id = self.chat_id;
        let message_id = self.message_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                api.set_reaction(chat_id, message_id, None).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    /// One recorded Bot API call.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Call {
        Reaction { emoji: Option<String> },
        Typing,
    }

    /// Fake sink that records every call and signals the test on each one, so
    /// tests can wait for the next call deterministically without sleeping.
    struct FakeApi {
        calls: Mutex<Vec<Call>>,
        tx: mpsc::UnboundedSender<Call>,
    }

    impl FakeApi {
        fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Call>) {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    calls: Mutex::new(Vec::new()),
                    tx,
                }),
                rx,
            )
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        fn record(&self, call: Call) {
            self.calls.lock().unwrap().push(call.clone());
            let _ = self.tx.send(call);
        }
    }

    #[async_trait]
    impl BotApi for FakeApi {
        async fn set_reaction(&self, _chat_id: i64, _message_id: i64, emoji: Option<&str>) {
            self.record(Call::Reaction {
                emoji: emoji.map(str::to_string),
            });
        }
        async fn send_chat_action(&self, _chat_id: i64, _action: &str) {
            self.record(Call::Typing);
        }
    }

    /// Wait for one recorded call or fail the test on timeout.
    async fn next_call(rx: &mut mpsc::UnboundedReceiver<Call>) -> Call {
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("expected a Bot API call before timeout")
            .expect("sink channel closed")
    }

    fn add_call() -> Call {
        Call::Reaction {
            emoji: Some(WORKING_EMOJI.to_string()),
        }
    }

    fn clear_call() -> Call {
        Call::Reaction { emoji: None }
    }

    #[tokio::test]
    async fn start_emits_reaction_add_and_typing() {
        let (api, mut rx) = FakeApi::new();
        let _guard = AckGuard::start(Arc::clone(&api) as Arc<dyn BotApi>, 1, 2).await;

        assert_eq!(next_call(&mut rx).await, add_call());
        assert_eq!(next_call(&mut rx).await, Call::Typing);
        assert_eq!(api.calls(), vec![add_call(), Call::Typing]);
    }

    #[tokio::test(start_paused = true)]
    async fn finish_clears_reaction_synchronously() {
        let (api, mut rx) = FakeApi::new();
        let guard = AckGuard::start(Arc::clone(&api) as Arc<dyn BotApi>, 1, 2).await;
        assert_eq!(next_call(&mut rx).await, add_call());
        assert_eq!(next_call(&mut rx).await, Call::Typing);

        // `finish` awaits the clear, so it is delivered by the time it returns
        // — no reliance on the runtime still being alive at drop.
        guard.finish().await;
        assert_eq!(api.calls().last(), Some(&clear_call()));
        assert_eq!(rx.recv().await, Some(clear_call()));

        // And no further typing refreshes after finishing.
        tokio::time::advance(TYPING_REFRESH * 3).await;
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err(), "no calls after finish");
        assert_eq!(
            api.calls().iter().filter(|c| **c == clear_call()).count(),
            1,
            "reaction cleared exactly once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drop_clears_reaction_and_stops_typing() {
        let (api, mut rx) = FakeApi::new();
        let guard = AckGuard::start(Arc::clone(&api) as Arc<dyn BotApi>, 1, 2).await;
        assert_eq!(next_call(&mut rx).await, add_call());
        assert_eq!(next_call(&mut rx).await, Call::Typing);

        drop(guard);
        // Let the spawned clear task run.
        tokio::task::yield_now().await;

        // The clear fires on drop.
        assert_eq!(rx.recv().await, Some(clear_call()));

        // After the clear, no more typing refreshes are emitted: well past
        // several refresh intervals there should be nothing further.
        tokio::time::advance(TYPING_REFRESH * 3).await;
        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "no further calls expected after drop"
        );

        let calls = api.calls();
        assert_eq!(calls.last(), Some(&clear_call()));
        assert_eq!(
            calls.iter().filter(|c| **c == clear_call()).count(),
            1,
            "reaction cleared exactly once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_is_refreshed_while_alive() {
        let (api, mut rx) = FakeApi::new();
        let _guard = AckGuard::start(Arc::clone(&api) as Arc<dyn BotApi>, 1, 2).await;
        assert_eq!(next_call(&mut rx).await, add_call());
        assert_eq!(next_call(&mut rx).await, Call::Typing);

        // Advancing past a refresh interval re-sends the typing action. After
        // advancing the paused clock the spawned refresh task runs; yielding
        // lets it record the call, which is then ready on the channel.
        for _ in 0..3 {
            tokio::time::advance(TYPING_REFRESH + Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(rx.recv().await, Some(Call::Typing));
        }
    }

    #[tokio::test]
    async fn clear_fires_on_panic_unwind() {
        let (api, mut rx) = FakeApi::new();
        let api_for_task = Arc::clone(&api);

        // A guard dropped during panic unwind must still clear the reaction.
        let result = tokio::spawn(async move {
            let _guard = AckGuard::start(api_for_task as Arc<dyn BotApi>, 1, 2).await;
            panic!("turn blew up");
        })
        .await;
        assert!(result.is_err(), "task should have panicked");

        // Drain calls; the clear must be present despite the panic.
        let mut saw_clear = false;
        while let Ok(call) = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            match call {
                Some(c) if c == clear_call() => {
                    saw_clear = true;
                    break;
                }
                Some(_) => continue,
                None => break,
            }
        }
        assert!(saw_clear, "reaction clear must fire on panic unwind");
        assert!(api.calls().contains(&clear_call()));
    }
}
