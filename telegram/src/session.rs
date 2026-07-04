//! Telegram-only session lifecycle: idle expiry plus `/new` and `/reset`
//! resets, layered on top of the per-chat chat session.
//!
//! Sessions are stored append-only by *generation*:
//!
//! ```text
//! data/telegram/sessions/<chat_id>/
//!   current.json            # pointer + metadata for the live generation
//!   <generation_id>/        # one directory per generation (never deleted)
//! ```
//!
//! The pointer records the current generation id and the unix time of the last
//! inbound Telegram message, so idle expiry survives restarts. Rotating to a
//! fresh generation only writes a new pointer + directory; prior generation
//! directories are always kept for audit/debug.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Default idle window before a Telegram chat session is considered stale.
pub const DEFAULT_IDLE_MINUTES: u64 = 360;

/// Notice sent before the next reply when an idle session is rotated.
pub const EXPIRED_NOTICE: &str = "Previous session expired; starting fresh.";

/// Reply sent for an explicit `/new` or `/reset`.
pub const RESET_REPLY: &str = "Context cleared, new session started.";

/// Telegram session configuration (`extensions.telegram.sessions`).
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct SessionsConfig {
    /// Idle minutes before a session expires. `0` disables idle expiry.
    pub idle_minutes: u64,
}

impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            idle_minutes: DEFAULT_IDLE_MINUTES,
        }
    }
}

/// A wall-clock source (unix seconds), abstracted so expiry is testable.
pub trait TimeSource {
    fn unix_secs(&self) -> u64;
}

/// Real wall clock backed by the system time.
pub struct SystemTime;

impl TimeSource for SystemTime {
    fn unix_secs(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// Persisted pointer to the current generation of a chat's session.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Pointer {
    /// Directory name of the current generation.
    generation: String,
    /// Unix seconds of the last inbound Telegram message.
    last_inbound: u64,
}

/// A Telegram command that resets the session, parsed from an inbound message.
///
/// Matching is exact-token only: the whole trimmed message must equal `/new`,
/// `/reset`, `/new@<bot>`, or `/reset@<bot>`. Anything with arguments (e.g.
/// `/new please`) is *not* a reset and flows through as a normal message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetCommand {
    New,
    Reset,
}

/// Classify an inbound message: is it an exact `/new` or `/reset` command?
///
/// A trailing `@bot` suffix (any non-empty bot name) is accepted so group-chat
/// command mentions work. Any whitespace/arguments after the token disqualify
/// it, so `/new please` is treated as an ordinary message.
pub fn parse_reset_command(text: &str) -> Option<ResetCommand> {
    let token = text.trim();
    // Reject anything with args: the whole trimmed message must be one token.
    if token.split_whitespace().count() != 1 {
        return None;
    }
    let base = match token.split_once('@') {
        Some((base, bot)) if !bot.is_empty() => base,
        Some(_) => return None, // trailing '@' with no bot name
        None => token,
    };
    match base {
        "/new" => Some(ResetCommand::New),
        "/reset" => Some(ResetCommand::Reset),
        _ => None,
    }
}

/// Whether an idle session should expire given the configured window.
///
/// `idle_minutes == 0` disables expiry entirely. Otherwise the session expires
/// once `now - last_inbound` reaches `idle_minutes` minutes.
fn is_expired(idle_minutes: u64, last_inbound: u64, now: u64) -> bool {
    if idle_minutes == 0 {
        return false;
    }
    now.saturating_sub(last_inbound) >= idle_minutes * 60
}

/// Per-chat session store managing append-only generations under a chat dir.
pub struct SessionStore {
    chat_dir: PathBuf,
}

/// Result of preparing a chat for an inbound message.
pub struct Prepared {
    /// Directory the chat session should open against (the current generation).
    pub session_dir: PathBuf,
    /// True when a stale generation was rotated for this message, so the caller
    /// should send [`EXPIRED_NOTICE`] and drop any live in-memory session.
    pub rotated: bool,
}

impl SessionStore {
    /// Open the store for one chat id under the sessions root.
    pub fn new(sessions_root: &Path, chat_id: i64) -> Self {
        Self {
            chat_dir: sessions_root.join(chat_id.to_string()),
        }
    }

    fn pointer_path(&self) -> PathBuf {
        self.chat_dir.join("current.json")
    }

    fn read_pointer(&self) -> Option<Pointer> {
        let raw = std::fs::read_to_string(self.pointer_path()).ok()?;
        serde_json::from_str(&raw).ok()
    }

    fn write_pointer(&self, pointer: &Pointer) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.chat_dir)?;
        let raw = serde_json::to_string(pointer).expect("pointer serializes");
        std::fs::write(self.pointer_path(), raw)
    }

    fn generation_dir(&self, generation: &str) -> PathBuf {
        self.chat_dir.join(generation)
    }

    /// Create a fresh generation directory and point the chat at it, stamping
    /// `last_inbound = now`. Returns the new generation's session directory.
    fn rotate(&self, now: u64) -> std::io::Result<PathBuf> {
        // Monotonic, collision-resistant id: unix seconds plus a short suffix so
        // two rotations in the same second still get distinct directories.
        let generation = format!("{now}-{}", short_suffix());
        let dir = self.generation_dir(&generation);
        std::fs::create_dir_all(&dir)?;
        self.write_pointer(&Pointer {
            generation,
            last_inbound: now,
        })?;
        Ok(dir)
    }

    /// Prepare the chat for an inbound message at `now`: rotate if the current
    /// generation is missing or idle-expired, else refresh `last_inbound` and
    /// reuse the live generation. The returned [`Prepared::rotated`] flags an
    /// idle-expiry rotation so the caller can post the expiry notice.
    pub fn prepare_inbound(&self, idle_minutes: u64, now: u64) -> std::io::Result<Prepared> {
        match self.read_pointer() {
            Some(pointer) if is_expired(idle_minutes, pointer.last_inbound, now) => {
                let session_dir = self.rotate(now)?;
                Ok(Prepared {
                    session_dir,
                    rotated: true,
                })
            }
            Some(pointer) => {
                // Live generation: keep it, just refresh the idle stamp.
                self.write_pointer(&Pointer {
                    generation: pointer.generation.clone(),
                    last_inbound: now,
                })?;
                Ok(Prepared {
                    session_dir: self.generation_dir(&pointer.generation),
                    rotated: false,
                })
            }
            None => {
                // First message for this chat: start a generation, no notice.
                let session_dir = self.rotate(now)?;
                Ok(Prepared {
                    session_dir,
                    rotated: false,
                })
            }
        }
    }

    /// Explicitly rotate to a fresh generation for `/new` or `/reset`, keeping
    /// prior generation directories. Returns the new session directory.
    pub fn reset(&self, now: u64) -> std::io::Result<PathBuf> {
        self.rotate(now)
    }
}

/// A short, filesystem-safe suffix that disambiguates two rotations landing in
/// the same unix second. A process-wide monotonic counter guarantees distinct
/// ids even for back-to-back rotations, so a fresh generation is never silently
/// merged into an existing directory.
fn short_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{n:06}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_reset_commands() {
        assert_eq!(parse_reset_command("/new"), Some(ResetCommand::New));
        assert_eq!(parse_reset_command("/reset"), Some(ResetCommand::Reset));
        assert_eq!(parse_reset_command("  /new  "), Some(ResetCommand::New));
    }

    #[test]
    fn parses_bot_suffixed_reset_commands() {
        assert_eq!(parse_reset_command("/new@mybot"), Some(ResetCommand::New));
        assert_eq!(
            parse_reset_command("/reset@some_bot"),
            Some(ResetCommand::Reset)
        );
    }

    #[test]
    fn command_with_args_is_not_a_reset() {
        assert_eq!(parse_reset_command("/new please"), None);
        assert_eq!(parse_reset_command("/reset now"), None);
        assert_eq!(parse_reset_command("/new@bot extra"), None);
    }

    #[test]
    fn unrelated_or_malformed_text_is_not_a_reset() {
        assert_eq!(parse_reset_command("hello"), None);
        assert_eq!(parse_reset_command("/newish"), None);
        assert_eq!(parse_reset_command("/new@"), None);
        assert_eq!(parse_reset_command("/start"), None);
        assert_eq!(parse_reset_command(""), None);
    }

    #[test]
    fn zero_idle_minutes_never_expires() {
        assert!(!is_expired(0, 0, u64::MAX));
    }

    #[test]
    fn default_window_expires_after_360_minutes() {
        let last = 1_000_000;
        // 359 minutes: still live.
        assert!(!is_expired(360, last, last + 359 * 60));
        // Exactly 360 minutes: expired.
        assert!(is_expired(360, last, last + 360 * 60));
        assert!(is_expired(360, last, last + 500 * 60));
    }

    fn tmp() -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("alg347-{}-{}", std::process::id(), short_suffix()));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn first_message_starts_generation_without_rotation_notice() {
        let root = tmp();
        let store = SessionStore::new(&root, 42);
        let prepared = store.prepare_inbound(360, 1_000).unwrap();
        assert!(
            !prepared.rotated,
            "first ever message must not notice-expire"
        );
        assert!(prepared.session_dir.is_dir());
    }

    #[test]
    fn live_session_is_reused_within_window() {
        let root = tmp();
        let store = SessionStore::new(&root, 7);
        let first = store.prepare_inbound(360, 1_000).unwrap();
        let again = store.prepare_inbound(360, 1_000 + 60).unwrap();
        assert!(!again.rotated);
        assert_eq!(first.session_dir, again.session_dir);
    }

    #[test]
    fn expired_session_rotates_and_flags_notice() {
        let root = tmp();
        let store = SessionStore::new(&root, 9);
        let first = store.prepare_inbound(360, 1_000).unwrap();
        let later = store.prepare_inbound(360, 1_000 + 360 * 60).unwrap();
        assert!(later.rotated, "idle-expired message must flag the notice");
        assert_ne!(first.session_dir, later.session_dir);
        // Old generation directory is kept.
        assert!(first.session_dir.is_dir());
    }

    #[test]
    fn zero_idle_minutes_never_rotates_on_prepare() {
        let root = tmp();
        let store = SessionStore::new(&root, 11);
        let first = store.prepare_inbound(0, 1_000).unwrap();
        let much_later = store.prepare_inbound(0, 1_000 + 10_000 * 60).unwrap();
        assert!(!much_later.rotated);
        assert_eq!(first.session_dir, much_later.session_dir);
    }

    #[test]
    fn reset_rotates_and_keeps_old_generation() {
        let root = tmp();
        let store = SessionStore::new(&root, 13);
        let first = store.prepare_inbound(360, 1_000).unwrap();
        // Write a marker into the old generation to prove it survives.
        let marker = first.session_dir.join("marker.txt");
        std::fs::write(&marker, "keep me").unwrap();

        let fresh = store.reset(2_000).unwrap();
        assert_ne!(first.session_dir, fresh);
        assert!(marker.is_file(), "reset must not delete old session data");

        // The next inbound reuses the reset generation, no notice.
        let after = store.prepare_inbound(360, 2_050).unwrap();
        assert!(!after.rotated);
        assert_eq!(fresh, after.session_dir);
    }
}
