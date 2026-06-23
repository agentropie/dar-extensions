//! The safety core. Per conversation, counts consecutive bot-authored turns since
//! the last human message and enforces a hard cap, after which the bot stays
//! silent until a human speaks again. Pure and deterministic — the most important
//! tested module.
//!
//! The guard applies to BOTH channels and DMs: a DM is just an unattended
//! conversation, and two agents DMing each other must hit the same cap as two
//! agents in a channel (the non-negotiable: no runaway bot-to-bot cost with no
//! human present). The caller keys each conversation distinctly (channel name vs.
//! a `dm:`-prefixed nick) so they never collide.
//!
//! Usage from the loop, per incoming message that would otherwise be answered:
//! call [`LoopGuard::should_respond`] with whether the *sender* is a bot and the
//! conversation key. It returns whether the bot is allowed to respond, and updates
//! the per-conversation counter as a side effect: a human sender resets it to
//! zero; a bot sender that is permitted to be answered increments it toward the
//! cap.

use std::collections::HashMap;

/// Tracks consecutive bot-to-bot turns per channel and enforces a hard cap.
#[derive(Debug)]
pub struct LoopGuard {
    /// Hard cap: maximum consecutive bot-authored turns the bot will answer with
    /// no intervening human message. `0` disables bot-to-bot replies entirely.
    max_bot_turns: u32,
    /// Per-channel count of consecutive bot turns since the last human message.
    counts: HashMap<String, u32>,
}

impl LoopGuard {
    pub fn new(max_bot_turns: u32) -> Self {
        Self {
            max_bot_turns,
            counts: HashMap::new(),
        }
    }

    /// Decide whether the bot may respond to a message in `channel` whose sender
    /// is (or isn't) another bot, updating the per-channel counter.
    ///
    /// - Human sender: reset the channel counter, always allow.
    /// - Bot sender: allow only if the consecutive-bot-turn count is below the
    ///   cap; on allow, increment. Once the cap is reached, stay silent until a
    ///   human resets the counter.
    pub fn should_respond(&mut self, sender_is_bot: bool, channel: &str) -> bool {
        if !sender_is_bot {
            self.counts.insert(channel.to_string(), 0);
            return true;
        }
        let count = self.counts.entry(channel.to_string()).or_insert(0);
        if *count >= self.max_bot_turns {
            false
        } else {
            *count += 1;
            true
        }
    }

    /// Record an inbound human message in `channel` without consuming a turn
    /// decision (used for ambient/context-only messages). Resets the counter.
    pub fn note_human(&mut self, channel: &str) {
        self.counts.insert(channel.to_string(), 0);
    }

    /// Current consecutive bot-turn count for a channel (for logging/tests).
    pub fn count(&self, channel: &str) -> u32 {
        self.counts.get(channel).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_sender_always_allowed_and_resets() {
        let mut g = LoopGuard::new(2);
        assert!(g.should_respond(false, "#room"));
        assert!(g.should_respond(false, "#room"));
        assert_eq!(g.count("#room"), 0);
    }

    #[test]
    fn cap_suppresses_after_consecutive_bot_turns() {
        let mut g = LoopGuard::new(2);
        // Two bot turns are allowed...
        assert!(g.should_respond(true, "#room"));
        assert!(g.should_respond(true, "#room"));
        // ...the third and beyond are suppressed.
        assert!(!g.should_respond(true, "#room"));
        assert!(!g.should_respond(true, "#room"));
    }

    #[test]
    fn human_message_resets_the_cap() {
        let mut g = LoopGuard::new(2);
        assert!(g.should_respond(true, "#room"));
        assert!(g.should_respond(true, "#room"));
        assert!(!g.should_respond(true, "#room")); // capped
        // A human speaks: counter resets.
        assert!(g.should_respond(false, "#room"));
        assert_eq!(g.count("#room"), 0);
        // Bot turns are allowed again, up to the cap.
        assert!(g.should_respond(true, "#room"));
        assert!(g.should_respond(true, "#room"));
        assert!(!g.should_respond(true, "#room"));
    }

    #[test]
    fn note_human_resets_without_a_turn() {
        let mut g = LoopGuard::new(3);
        assert!(g.should_respond(true, "#room"));
        assert!(g.should_respond(true, "#room"));
        assert_eq!(g.count("#room"), 2);
        g.note_human("#room");
        assert_eq!(g.count("#room"), 0);
    }

    #[test]
    fn per_channel_isolation() {
        let mut g = LoopGuard::new(1);
        // Cap out #a.
        assert!(g.should_respond(true, "#a"));
        assert!(!g.should_respond(true, "#a"));
        // #b is unaffected.
        assert!(g.should_respond(true, "#b"));
        assert!(!g.should_respond(true, "#b"));
        // A human in #a does not touch #b's (already capped) state.
        assert!(g.should_respond(false, "#a"));
        assert!(!g.should_respond(true, "#b"));
        assert!(g.should_respond(true, "#a"));
    }

    #[test]
    fn dm_and_channel_keys_are_isolated() {
        // DMs ARE guarded (the loop keys them as `dm:<nick>`), but each
        // conversation is an independent bucket, so a capped channel never mutes a
        // DM and vice versa. Capping `#room` leaves the `dm:alice` key fresh.
        let mut g = LoopGuard::new(1);
        assert!(g.should_respond(true, "#room"));
        assert!(!g.should_respond(true, "#room"));
        assert!(g.should_respond(true, "dm:alice"));
    }

    #[test]
    fn zero_cap_blocks_all_bot_turns() {
        let mut g = LoopGuard::new(0);
        assert!(!g.should_respond(true, "#room"));
        // Humans still allowed.
        assert!(g.should_respond(false, "#room"));
        assert!(!g.should_respond(true, "#room"));
    }

    #[test]
    fn interleaved_human_and_bot_keeps_counter_bounded() {
        let mut g = LoopGuard::new(2);
        for _ in 0..100 {
            // Every bot turn is preceded by a human one => never caps.
            assert!(g.should_respond(false, "#room"));
            assert!(g.should_respond(true, "#room"));
        }
        assert_eq!(g.count("#room"), 1);
    }

    #[test]
    fn unknown_channel_count_is_zero() {
        let g = LoopGuard::new(2);
        assert_eq!(g.count("#never-seen"), 0);
    }
}
