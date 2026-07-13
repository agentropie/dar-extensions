use std::path::{Path, PathBuf};

use crate::addressing::{ConversationKind, InboundMessage};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationKey(String);

impl ConversationKey {
    pub fn from_message(message: &InboundMessage<'_>) -> Self {
        let kind = match message.kind {
            ConversationKind::DirectMessage => match message.thread_ts {
                Some(thread) => format!("dm:{}:thread:{thread}", message.sender_id),
                None => format!("dm:{}:root", message.sender_id),
            },
            ConversationKind::Channel => match message.thread_ts {
                Some(thread) => format!("channel:{}:thread:{thread}", message.channel_id),
                None => format!("channel:{}:root", message.channel_id),
            },
        };
        Self(format!("workspace:{}:{kind}", message.team_id))
    }

    pub fn directory(&self, data_dir: &Path) -> PathBuf {
        data_dir.join("sessions").join(hex(self.0.as_bytes()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addressing::{ConversationKind, InboundMessage};

    fn message<'a>(
        kind: ConversationKind,
        sender: &'a str,
        channel: &'a str,
        thread: Option<&'a str>,
    ) -> InboundMessage<'a> {
        InboundMessage {
            team_id: "T/../unsafe",
            channel_id: channel,
            sender_id: sender,
            text: "x",
            bot_user_id: None,
            thread_ts: thread,
            message_ts: "1",
            kind,
        }
    }

    #[test]
    fn separates_dms_by_workspace_and_user() {
        let first = ConversationKey::from_message(&message(
            ConversationKind::DirectMessage,
            "U1",
            "D1",
            None,
        ));
        let second = ConversationKey::from_message(&message(
            ConversationKind::DirectMessage,
            "U2",
            "D2",
            None,
        ));
        assert_ne!(first, second);
        assert_ne!(
            first.directory(Path::new("data")),
            second.directory(Path::new("data"))
        );
    }

    #[test]
    fn separates_channel_root_and_thread() {
        let root =
            ConversationKey::from_message(&message(ConversationKind::Channel, "U1", "C1", None));
        let thread = ConversationKey::from_message(&message(
            ConversationKind::Channel,
            "U1",
            "C1",
            Some("1.1"),
        ));
        assert_ne!(root, thread);
    }

    #[test]
    fn separates_dm_root_and_thread_sessions() {
        let root = ConversationKey::from_message(&message(
            ConversationKind::DirectMessage,
            "U1",
            "D1",
            None,
        ));
        let thread = ConversationKey::from_message(&message(
            ConversationKind::DirectMessage,
            "U1",
            "D1",
            Some("1.1"),
        ));
        assert_eq!(root.as_str(), "workspace:T/../unsafe:dm:U1:root");
        assert_eq!(thread.as_str(), "workspace:T/../unsafe:dm:U1:thread:1.1");
    }

    #[test]
    fn directory_cannot_contain_slack_path_segments() {
        let key = ConversationKey::from_message(&message(
            ConversationKind::DirectMessage,
            "U/../x",
            "D",
            None,
        ));
        let directory = key.directory(Path::new("data"));
        assert_eq!(directory.components().count(), 3);
        assert!(!directory.to_string_lossy().contains(".."));
    }
}
