use regex::Regex;

use crate::config::{ChannelConfig, SlackConfig, ThreadPolicy};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConversationKind {
    DirectMessage,
    Channel,
}

#[derive(Clone, Debug)]
pub struct InboundMessage<'a> {
    pub team_id: &'a str,
    pub channel_id: &'a str,
    pub sender_id: &'a str,
    pub text: &'a str,
    pub has_files: bool,
    pub bot_user_id: Option<&'a str>,
    pub thread_ts: Option<&'a str>,
    pub message_ts: &'a str,
    pub kind: ConversationKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteDecision {
    Ignore,
    Dispatch {
        text: String,
        reply_thread_ts: Option<String>,
    },
}

pub fn route(
    config: &SlackConfig,
    patterns: &[Regex],
    message: &InboundMessage<'_>,
) -> RouteDecision {
    if message.team_id.is_empty()
        || message.channel_id.is_empty()
        || message.sender_id.is_empty()
        || message.sender_id == message.bot_user_id.unwrap_or_default()
    {
        return RouteDecision::Ignore;
    }

    match message.kind {
        ConversationKind::DirectMessage => {
            if !config.dm.enabled || !allowed(&config.dm.users, message.sender_id) {
                return RouteDecision::Ignore;
            }
            RouteDecision::Dispatch {
                text: message.text.to_owned(),
                reply_thread_ts: reply_thread_ts(
                    config.dm.thread_policy,
                    message.thread_ts,
                    message.message_ts,
                ),
            }
        }
        ConversationKind::Channel => route_channel(config, patterns, message),
    }
}

fn route_channel(
    config: &SlackConfig,
    patterns: &[Regex],
    message: &InboundMessage<'_>,
) -> RouteDecision {
    let channel = match config.channels.get(message.channel_id) {
        Some(channel) => channel.clone(),
        None if config.channels.is_empty() => ChannelConfig::default(),
        None => return RouteDecision::Ignore,
    };
    if !allowed(&channel.users, message.sender_id) {
        return RouteDecision::Ignore;
    }

    let (mentioned, text) = strip_mention(message.text, message.bot_user_id, patterns);
    if channel.require_mention
        && !mentioned
        && !(message.has_files && message.text.trim().is_empty())
    {
        return RouteDecision::Ignore;
    }

    let reply_thread_ts =
        reply_thread_ts(channel.thread_policy, message.thread_ts, message.message_ts);
    RouteDecision::Dispatch {
        text,
        reply_thread_ts,
    }
}

fn reply_thread_ts(
    policy: ThreadPolicy,
    thread_ts: Option<&str>,
    message_ts: &str,
) -> Option<String> {
    match policy {
        ThreadPolicy::Always => Some(thread_ts.unwrap_or(message_ts).to_owned()),
        ThreadPolicy::Never => None,
        ThreadPolicy::Follow => thread_ts.map(str::to_owned),
    }
}

fn allowed(users: &[String], user: &str) -> bool {
    users.is_empty() || users.iter().any(|allowed| allowed == user)
}

pub fn strip_mention(text: &str, bot_user_id: Option<&str>, patterns: &[Regex]) -> (bool, String) {
    if let Some(user_id) = bot_user_id {
        let mention = format!("<@{user_id}>");
        if let Some(position) = text.find(&mention) {
            let mut stripped = text.to_owned();
            stripped.replace_range(position..position + mention.len(), "");
            return (true, stripped.trim().to_owned());
        }
    }
    for pattern in patterns {
        if let Some(found) = pattern.find(text) {
            let mut stripped = text.to_owned();
            stripped.replace_range(found.range(), "");
            return (true, stripped.trim().to_owned());
        }
    }
    (false, text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ChannelConfig, SlackConfig};
    use std::collections::HashMap;

    fn channel_config() -> SlackConfig {
        SlackConfig {
            channels: HashMap::from([("C1".into(), ChannelConfig::default())]),
            ..SlackConfig::default()
        }
    }

    fn channel<'a>(text: &'a str, thread_ts: Option<&'a str>) -> InboundMessage<'a> {
        InboundMessage {
            team_id: "T1",
            channel_id: "C1",
            sender_id: "U1",
            text,
            has_files: false,
            bot_user_id: Some("B1"),
            thread_ts,
            message_ts: "1.2",
            kind: ConversationKind::Channel,
        }
    }

    #[test]
    fn channel_requires_and_removes_mention() {
        assert_eq!(
            route(&channel_config(), &[], &channel("hello", None)),
            RouteDecision::Ignore
        );
        assert_eq!(
            route(&channel_config(), &[], &channel("<@B1> hello", None)),
            RouteDecision::Dispatch {
                text: "hello".into(),
                reply_thread_ts: Some("1.2".into())
            }
        );
    }

    #[test]
    fn attachment_only_channel_bypasses_mention_requirement() {
        let mut attachment = channel("", None);
        attachment.has_files = true;
        assert!(matches!(
            route(&channel_config(), &[], &attachment),
            RouteDecision::Dispatch { text, .. } if text.is_empty()
        ));
        assert_eq!(
            route(&channel_config(), &[], &channel("", None)),
            RouteDecision::Ignore
        );
        let mut captioned_attachment = channel("caption", None);
        captioned_attachment.has_files = true;
        assert_eq!(
            route(&channel_config(), &[], &captioned_attachment),
            RouteDecision::Ignore
        );
    }

    #[test]
    fn configured_channels_reject_absent_channel() {
        let mut message = channel("<@B1> hello", None);
        message.channel_id = "C2";
        assert_eq!(
            route(&channel_config(), &[], &message),
            RouteDecision::Ignore
        );
    }

    #[test]
    fn denied_dm_is_not_routed() {
        let config = SlackConfig {
            dm: crate::config::DirectMessageConfig {
                enabled: true,
                users: vec!["U2".into()],
                ..Default::default()
            },
            ..SlackConfig::default()
        };
        let message = InboundMessage {
            kind: ConversationKind::DirectMessage,
            ..channel("private", None)
        };
        assert_eq!(route(&config, &[], &message), RouteDecision::Ignore);
    }

    fn direct_message<'a>(text: &'a str, thread_ts: Option<&'a str>) -> InboundMessage<'a> {
        InboundMessage {
            kind: ConversationKind::DirectMessage,
            ..channel(text, thread_ts)
        }
    }

    #[test]
    fn dm_default_routes_root_directly_and_follows_existing_thread() {
        let mut config = SlackConfig::default();
        config.dm.enabled = true;

        assert_eq!(
            route(&config, &[], &direct_message("hello", None)),
            RouteDecision::Dispatch {
                text: "hello".into(),
                reply_thread_ts: None,
            }
        );
        assert_eq!(
            route(&config, &[], &direct_message("hello", Some("9.0"))),
            RouteDecision::Dispatch {
                text: "hello".into(),
                reply_thread_ts: Some("9.0".into()),
            }
        );
    }

    #[test]
    fn dm_explicit_thread_policies_control_reply_placement() {
        for (policy, root, thread) in [
            (ThreadPolicy::Always, Some("1.2"), Some("9.0")),
            (ThreadPolicy::Never, None, None),
        ] {
            let mut config = SlackConfig::default();
            config.dm.enabled = true;
            config.dm.thread_policy = policy;
            assert_eq!(
                route(&config, &[], &direct_message("hello", None)),
                RouteDecision::Dispatch {
                    text: "hello".into(),
                    reply_thread_ts: root.map(str::to_owned),
                }
            );
            assert_eq!(
                route(&config, &[], &direct_message("hello", Some("9.0"))),
                RouteDecision::Dispatch {
                    text: "hello".into(),
                    reply_thread_ts: thread.map(str::to_owned),
                }
            );
        }
    }

    #[test]
    fn thread_policy_matrix() {
        for (policy, root, thread) in [
            (ThreadPolicy::Always, Some("1.2"), Some("9.0")),
            (ThreadPolicy::Never, None, None),
            (ThreadPolicy::Follow, None, Some("9.0")),
        ] {
            let mut config = channel_config();
            config.channels.get_mut("C1").unwrap().thread_policy = policy;
            let root_result = route(&config, &[], &channel("<@B1> x", None));
            let thread_result = route(&config, &[], &channel("<@B1> x", Some("9.0")));
            assert_eq!(
                root_result,
                RouteDecision::Dispatch {
                    text: "x".into(),
                    reply_thread_ts: root.map(str::to_owned)
                }
            );
            assert_eq!(
                thread_result,
                RouteDecision::Dispatch {
                    text: "x".into(),
                    reply_thread_ts: thread.map(str::to_owned)
                }
            );
        }
    }
}
