//! Live reply streaming and tool-status presentation for a Telegram turn.
//!
//! A turn used to be silent until it finished: assistant text was buffered and
//! sent once at the end, so during a long or stuck tool run the chat showed
//! only the ack + typing. This module streams the turn as it happens using two
//! editable message bubbles:
//!
//! * **the answer bubble** — the first assistant text sends one message; later
//!   text edits that same message in place, rate-limited so we don't spam every
//!   token or trip Telegram's edit/flood limits.
//! * **the tool-status bubble** — a separate message showing the tool currently
//!   running (name + a short target/preview), kept out of the answer so tool
//!   metadata never pollutes the reply.
//!
//! At a tool boundary the visible answer text is finalized before the tool is
//! awaited, so a tool hang can never hide text the assistant already produced.
//! When the turn finishes, the status bubble collapses into a concise summary
//! (`Used 3 tools: read, bash, edit`) and the answer bubble is finalized with
//! the full reply.
//!
//! The Telegram surface is abstracted behind [`StreamApi`] so the streaming
//! state machine can be tested against a fake sink without any network, exactly
//! like the acknowledgement guard's `BotApi`.

use std::time::Duration;

use async_trait::async_trait;

/// Balanced edit cadence: don't edit the answer bubble more than once per this
/// interval unless a boundary (tool call / turn finish) forces an immediate
/// flush. Keeps live updates visible without hammering Telegram's edit limits.
pub const EDIT_INTERVAL: Duration = Duration::from_secs(1);
/// ...or when this many new visible characters have accrued since the last
/// edit, whichever comes first. Bursty output flushes on size, idle output on
/// time.
pub const EDIT_CHAR_THRESHOLD: usize = 200;
/// Max length of the short per-tool target/preview shown in the status bubble.
/// Enough to identify the target (a path, a command) without dumping full args.
const TOOL_PREVIEW_MAX: usize = 80;

/// Outcome of an edit attempt. A failed edit must never abort the turn: the
/// caller falls back to plain delivery so the final answer always lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EditResult {
    Ok,
    Failed,
}

/// The Telegram message surface the streamer needs. Abstracted as a trait so
/// tests inject a fake sink and assert the externally visible behavior — which
/// messages were sent, edited, and with what text — without touching the
/// network.
#[async_trait]
pub trait StreamApi: Send + Sync {
    /// Send a new message and return its message id (or `None` on failure).
    async fn send(&self, text: &str) -> Option<i64>;
    /// Edit an existing message's text in place. Returns whether it succeeded;
    /// a failure triggers the caller's fallback to a fresh send.
    async fn edit(&self, message_id: i64, text: &str) -> EditResult;
}

/// A short one-line preview of a tool call for the status bubble: the tool name
/// plus a compact target extracted from its arguments (a path, a command, a
/// query), never the full argument JSON.
pub fn tool_preview(name: &str, args: &str) -> String {
    match short_target(args) {
        Some(target) => format!("{name} · {target}"),
        None => name.to_string(),
    }
}

/// Pull a short human target out of a tool's rendered args. Prefers the common
/// identifying fields (path/file/command/query/…); otherwise falls back to the
/// first scalar string value; otherwise nothing. Always truncated so the status
/// line stays compact.
fn short_target(args: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(args).ok()?;
    let obj = value.as_object()?;
    const PREFERRED: &[&str] = &[
        "path",
        "file",
        "file_path",
        "filename",
        "cmd",
        "command",
        "query",
        "pattern",
        "url",
        "name",
        "target",
    ];
    let pick = PREFERRED
        .iter()
        .find_map(|k| obj.get(*k).and_then(scalar_str))
        .or_else(|| obj.values().find_map(scalar_str));
    pick.map(|s| truncate(&collapse_ws(&s), TOOL_PREVIEW_MAX))
}

/// Render a JSON scalar as a short string; skip objects/arrays/null so the
/// preview never becomes nested JSON.
fn scalar_str(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Collapse internal whitespace/newlines to single spaces so a multi-line arg
/// (e.g. a heredoc command) renders as one tidy status line.
fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Truncate to at most `max` characters, appending an ellipsis when cut.
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}…")
}

/// A used-tools summary line for the collapsed status bubble, e.g.
/// `Used 3 tools: read, bash, edit`. Preserves call order and de-duplicates
/// repeated names so a loop of the same tool reads cleanly.
pub fn tool_summary(names: &[String]) -> String {
    if names.is_empty() {
        return String::new();
    }
    let mut unique: Vec<&str> = Vec::new();
    for n in names {
        if !unique.contains(&n.as_str()) {
            unique.push(n.as_str());
        }
    }
    let count = names.len();
    let noun = if count == 1 { "tool" } else { "tools" };
    format!("Used {count} {noun}: {}", unique.join(", "))
}

/// Drives the two live bubbles for one turn against a [`StreamApi`].
///
/// The state machine is deliberately UI-only: it never touches agent
/// conversation history. Feed it assistant text via [`push_text`], mark tool
/// boundaries via [`tool_started`], and finalize via [`finish`]. It decides
/// when to send vs. edit, when to flush at boundaries, and how to collapse the
/// status bubble.
///
/// [`push_text`]: LiveTurn::push_text
/// [`tool_started`]: LiveTurn::tool_started
/// [`finish`]: LiveTurn::finish
pub struct LiveTurn<'a, C: Clock> {
    api: &'a dyn StreamApi,
    clock: C,
    /// The live answer bubble's message id, once created.
    answer_msg: Option<i64>,
    /// Full assistant text accumulated so far (all segments concatenated).
    answer: String,
    /// Answer length (chars) last written to the bubble; used for cadence.
    last_written_len: usize,
    /// When the answer bubble was last edited, for the time-based cadence.
    last_edit: Option<C::Instant>,
    /// The status bubble's message id, once a tool has run.
    status_msg: Option<i64>,
    /// Tool names in call order, for the collapsed summary.
    tools: Vec<String>,
}

/// A minimal clock so the time-based edit cadence is testable with a fake.
pub trait Clock {
    type Instant: Copy;
    fn now(&self) -> Self::Instant;
    fn elapsed_since(&self, earlier: Self::Instant) -> Duration;
}

/// Real monotonic clock backed by `std::time::Instant`.
pub struct RealClock;

impl Clock for RealClock {
    type Instant = std::time::Instant;
    fn now(&self) -> Self::Instant {
        std::time::Instant::now()
    }
    fn elapsed_since(&self, earlier: Self::Instant) -> Duration {
        earlier.elapsed()
    }
}

impl<'a, C: Clock> LiveTurn<'a, C> {
    pub fn new(api: &'a dyn StreamApi, clock: C) -> Self {
        Self {
            api,
            clock,
            answer_msg: None,
            answer: String::new(),
            last_written_len: 0,
            last_edit: None,
            status_msg: None,
            tools: Vec::new(),
        }
    }

    /// The full accumulated assistant text so far.
    pub fn answer(&self) -> &str {
        &self.answer
    }

    /// Append streamed assistant text and update the live bubble if the cadence
    /// allows. The first non-empty text creates the bubble; later text edits it
    /// in place, but only when enough time has passed or enough new characters
    /// have accrued — so we stream visibly without editing on every token.
    pub async fn push_text(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.answer.push_str(text);
        if self.answer_msg.is_none() {
            // First visible text: create the single answer bubble.
            self.answer_msg = self.api.send(&self.answer).await;
            self.last_written_len = self.answer.chars().count();
            self.last_edit = Some(self.clock.now());
            return;
        }
        if self.should_edit() {
            self.flush_answer().await;
        }
    }

    /// Whether the accrued delta since the last write crosses either cadence
    /// threshold (time or characters).
    fn should_edit(&self) -> bool {
        let grown = self
            .answer
            .chars()
            .count()
            .saturating_sub(self.last_written_len);
        if grown == 0 {
            return false;
        }
        if grown >= EDIT_CHAR_THRESHOLD {
            return true;
        }
        match self.last_edit {
            Some(t) => self.clock.elapsed_since(t) >= EDIT_INTERVAL,
            None => true,
        }
    }

    /// Force the answer bubble to reflect all accumulated text right now,
    /// bypassing the cadence. Used at tool boundaries and turn finish.
    async fn flush_answer(&mut self) {
        let Some(id) = self.answer_msg else {
            // Nothing sent yet; create the bubble if we have text.
            if !self.answer.is_empty() {
                self.answer_msg = self.api.send(&self.answer).await;
                self.last_written_len = self.answer.chars().count();
                self.last_edit = Some(self.clock.now());
            }
            return;
        };
        if self.answer.chars().count() == self.last_written_len {
            return;
        }
        let _ = self.api.edit(id, &self.answer).await;
        self.last_written_len = self.answer.chars().count();
        self.last_edit = Some(self.clock.now());
    }

    /// A tool is about to run. Finalize the visible answer text first (so a tool
    /// hang can't hide it), then show/update the status bubble with the tool
    /// name and a short target/preview.
    pub async fn tool_started(&mut self, name: &str, args: &str) {
        self.flush_answer().await;
        self.tools.push(name.to_string());
        let line = tool_preview(name, args);
        match self.status_msg {
            Some(id) => {
                let _ = self.api.edit(id, &line).await;
            }
            None => {
                self.status_msg = self.api.send(&line).await;
            }
        }
    }

    /// Finish the turn: collapse the status bubble into a used-tools summary
    /// (or leave it untouched if no tool ran) and force one last answer flush.
    /// The final rich-markdown delivery is handled by the caller; this only
    /// ensures the streamed preview reflects the complete text and the status
    /// bubble is tidy.
    pub async fn finish(&mut self) {
        self.flush_answer().await;
        if let Some(id) = self.status_msg {
            let summary = tool_summary(&self.tools);
            if !summary.is_empty() {
                let _ = self.api.edit(id, &summary).await;
            }
        }
    }

    /// The answer bubble's message id, if one was created. Lets the caller
    /// finalize the same bubble with rich markdown (or delete it and re-send)
    /// so the final answer doesn't duplicate the streamed preview.
    pub fn answer_message_id(&self) -> Option<i64> {
        self.answer_msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::Mutex;

    /// One recorded Telegram surface call.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Call {
        Send { text: String },
        Edit { id: i64, text: String },
    }

    /// Fake sink recording every send/edit. `edit_fails` forces edits to fail
    /// so the fallback path can be exercised.
    struct FakeApi {
        calls: Mutex<Vec<Call>>,
        next_id: Mutex<i64>,
        edit_fails: bool,
    }

    impl FakeApi {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                next_id: Mutex::new(100),
                edit_fails: false,
            }
        }
        fn failing_edits() -> Self {
            Self {
                edit_fails: true,
                ..Self::new()
            }
        }
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
        fn sends(&self) -> Vec<String> {
            self.calls()
                .into_iter()
                .filter_map(|c| match c {
                    Call::Send { text } => Some(text),
                    _ => None,
                })
                .collect()
        }
        fn edits(&self) -> Vec<(i64, String)> {
            self.calls()
                .into_iter()
                .filter_map(|c| match c {
                    Call::Edit { id, text } => Some((id, text)),
                    _ => None,
                })
                .collect()
        }
    }

    #[async_trait]
    impl StreamApi for FakeApi {
        async fn send(&self, text: &str) -> Option<i64> {
            self.calls.lock().unwrap().push(Call::Send {
                text: text.to_string(),
            });
            let mut id = self.next_id.lock().unwrap();
            let this = *id;
            *id += 1;
            Some(this)
        }
        async fn edit(&self, message_id: i64, text: &str) -> EditResult {
            self.calls.lock().unwrap().push(Call::Edit {
                id: message_id,
                text: text.to_string(),
            });
            if self.edit_fails {
                EditResult::Failed
            } else {
                EditResult::Ok
            }
        }
    }

    /// Controllable clock: `now` only advances when the test calls `advance`,
    /// so cadence is deterministic.
    struct FakeClock {
        now: Cell<u64>,
    }
    impl FakeClock {
        fn new() -> Self {
            Self { now: Cell::new(0) }
        }
        fn advance(&self, by: Duration) {
            self.now.set(self.now.get() + by.as_millis() as u64);
        }
    }
    impl Clock for FakeClock {
        type Instant = u64;
        fn now(&self) -> u64 {
            self.now.get()
        }
        fn elapsed_since(&self, earlier: u64) -> Duration {
            Duration::from_millis(self.now.get() - earlier)
        }
    }

    #[tokio::test]
    async fn first_delta_sends_then_deltas_edit_in_place() {
        let api = FakeApi::new();
        let clock = FakeClock::new();
        let mut turn = LiveTurn::new(&api, clock);

        turn.push_text("Hello").await;
        // Second delta within the cadence window with < threshold chars: no edit.
        turn.push_text(" there").await;
        // Force cadence by time, then a delta should edit the same bubble.
        turn.clock.advance(EDIT_INTERVAL);
        turn.push_text("!").await;

        let sends = api.sends();
        assert_eq!(sends.len(), 1, "exactly one send: the initial bubble");
        assert_eq!(sends[0], "Hello");
        let edits = api.edits();
        assert_eq!(edits.len(), 1, "one rate-limited edit after the interval");
        assert_eq!(edits[0], (100, "Hello there!".to_string()));
    }

    #[tokio::test]
    async fn char_threshold_forces_edit_before_interval() {
        let api = FakeApi::new();
        let mut turn = LiveTurn::new(&api, FakeClock::new());
        turn.push_text("start").await;
        // No time advance, but push past the char threshold: must edit.
        let big = "x".repeat(EDIT_CHAR_THRESHOLD);
        turn.push_text(&big).await;
        assert_eq!(api.edits().len(), 1, "char threshold triggers an edit");
    }

    #[tokio::test]
    async fn tool_boundary_flushes_answer_before_status() {
        let api = FakeApi::new();
        let mut turn = LiveTurn::new(&api, FakeClock::new());
        turn.push_text("Let me check").await; // send
        turn.push_text(" the file").await; // buffered (cadence not met)
        turn.tool_started("read", r#"{"path":"/etc/hosts"}"#).await;

        let calls = api.calls();
        // Order: send(answer), edit(answer flush), send(status).
        assert_eq!(
            calls[0],
            Call::Send {
                text: "Let me check".to_string()
            }
        );
        assert_eq!(
            calls[1],
            Call::Edit {
                id: 100,
                text: "Let me check the file".to_string()
            },
            "pre-tool assistant text is flushed before the tool status appears"
        );
        assert_eq!(
            calls[2],
            Call::Send {
                text: "read · /etc/hosts".to_string()
            },
            "status shows tool name + short target, not full JSON"
        );
    }

    #[tokio::test]
    async fn tool_status_does_not_show_full_args_json() {
        let api = FakeApi::new();
        let mut turn = LiveTurn::new(&api, FakeClock::new());
        turn.tool_started("bash", r#"{"command":"ls -la /tmp","timeout":30}"#)
            .await;
        let status = api.sends().pop().unwrap();
        assert_eq!(status, "bash · ls -la /tmp");
        assert!(!status.contains('{'), "no raw JSON in the status line");
    }

    #[tokio::test]
    async fn finish_collapses_status_to_used_tools_summary() {
        let api = FakeApi::new();
        let mut turn = LiveTurn::new(&api, FakeClock::new());
        turn.tool_started("read", r#"{"path":"a"}"#).await;
        turn.tool_started("bash", r#"{"command":"ls"}"#).await;
        turn.tool_started("read", r#"{"path":"b"}"#).await;
        turn.finish().await;

        let edits = api.edits();
        let last = edits.last().unwrap();
        assert_eq!(
            last.1, "Used 3 tools: read, bash",
            "status collapses to a de-duplicated used-tools summary"
        );
    }

    #[tokio::test]
    async fn edit_failure_does_not_panic_and_answer_is_preserved() {
        let api = FakeApi::failing_edits();
        let mut turn = LiveTurn::new(&api, FakeClock::new());
        turn.push_text("first").await;
        turn.clock.advance(EDIT_INTERVAL);
        turn.push_text(" second").await; // edit attempted, fails
        turn.finish().await;
        // The full text is still tracked for the caller's final delivery.
        assert_eq!(turn.answer(), "first second");
    }

    #[test]
    fn tool_summary_singular_and_plural() {
        assert_eq!(tool_summary(&[]), "");
        assert_eq!(tool_summary(&["read".to_string()]), "Used 1 tool: read");
        assert_eq!(
            tool_summary(&["read".to_string(), "bash".to_string()]),
            "Used 2 tools: read, bash"
        );
    }

    #[test]
    fn tool_preview_prefers_identifying_field_and_truncates() {
        assert_eq!(
            tool_preview("read", r#"{"path":"/etc/hosts"}"#),
            "read · /etc/hosts"
        );
        // No recognizable field, no scalar: name only.
        assert_eq!(tool_preview("noop", "{}"), "noop");
        // Unparseable args: name only.
        assert_eq!(tool_preview("weird", "not json"), "weird");
        // Long target is truncated with an ellipsis.
        let long = format!(r#"{{"path":"{}"}}"#, "a".repeat(200));
        let out = tool_preview("read", &long);
        assert!(out.chars().count() <= "read · ".chars().count() + TOOL_PREVIEW_MAX);
        assert!(out.ends_with('…'));
    }
}
