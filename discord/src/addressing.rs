use crate::{config::DiscordConfig, session::SessionKey};

#[derive(Debug)]
pub struct InboundMessage<'a> {
    pub guild_id: Option<&'a str>,
    pub channel_id: &'a str,
    pub author_id: &'a str,
    pub author_is_bot: bool,
    pub webhook_id: Option<&'a str>,
    pub text: &'a str,
    pub has_attachments: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub enum RouteDecision {
    Ignore,
    Dispatch {
        text: String,
        session_key: SessionKey,
    },
}

pub fn route(
    config: &DiscordConfig,
    bot_user_id: Option<&str>,
    message: &InboundMessage<'_>,
) -> RouteDecision {
    if message.author_is_bot
        || message.webhook_id.is_some()
        || message.channel_id.is_empty()
        || message.author_id.is_empty()
    {
        return RouteDecision::Ignore;
    }
    let Some(guild_id) = message.guild_id else {
        return RouteDecision::Dispatch {
            text: message.text.trim().to_owned(),
            session_key: SessionKey::dm(message.author_id),
        };
    };
    let Some(guild) = config.guilds.get(guild_id) else {
        return RouteDecision::Ignore;
    };
    let Some(channel) = guild.channels.get(message.channel_id) else {
        return RouteDecision::Ignore;
    };
    if !guild.enabled
        || !channel.enabled
        || !allowed(&guild.users, message.author_id)
        || !allowed(&channel.users, message.author_id)
    {
        return RouteDecision::Ignore;
    }
    let (mentioned, text) = strip_mention(message.text, bot_user_id);
    if channel.require_mention
        && !mentioned
        && !(message.text.trim().is_empty() && message.has_attachments)
    {
        return RouteDecision::Ignore;
    }
    RouteDecision::Dispatch {
        text,
        session_key: SessionKey::guild_channel(guild_id, message.channel_id),
    }
}

fn allowed(users: &[String], user: &str) -> bool {
    users.is_empty() || users.iter().any(|id| id == user)
}

fn strip_mention(text: &str, bot_user_id: Option<&str>) -> (bool, String) {
    let Some(id) = bot_user_id else {
        return (false, text.trim().to_owned());
    };
    let mentions = [format!("<@{id}>"), format!("<@!{id}>")];
    let mut stripped = text.to_owned();
    let mut found = false;
    for mention in mentions {
        if stripped.contains(&mention) {
            found = true;
            stripped = stripped.replace(&mention, "");
        }
    }
    (found, stripped.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ChannelConfig, GuildConfig};
    use std::collections::HashMap;

    fn config() -> DiscordConfig {
        DiscordConfig {
            guilds: HashMap::from([(
                "g1".into(),
                GuildConfig {
                    channels: HashMap::from([
                        ("c1".into(), ChannelConfig::default()),
                        ("thread".into(), ChannelConfig::default()),
                    ]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }
    fn message<'a>(
        guild_id: Option<&'a str>,
        channel_id: &'a str,
        text: &'a str,
    ) -> InboundMessage<'a> {
        InboundMessage {
            guild_id,
            channel_id,
            author_id: "u1",
            author_is_bot: false,
            webhook_id: None,
            text,
            has_attachments: false,
        }
    }

    #[test]
    fn gating_matrix_handles_mentions_dms_threads_allowlists_and_bots() {
        let cfg = config();
        assert_eq!(
            route(&cfg, Some("b1"), &message(Some("g1"), "c1", "hello")),
            RouteDecision::Ignore
        );
        assert_eq!(
            route(&cfg, Some("b1"), &message(None, "dm", "hello")),
            RouteDecision::Dispatch {
                text: "hello".into(),
                session_key: SessionKey::dm("u1")
            }
        );
        assert_eq!(
            route(
                &cfg,
                Some("b1"),
                &message(Some("g1"), "thread", "<@b1> hello")
            ),
            RouteDecision::Dispatch {
                text: "hello".into(),
                session_key: SessionKey::guild_channel("g1", "thread")
            }
        );
        let mut restricted = cfg.clone();
        restricted
            .guilds
            .get_mut("g1")
            .unwrap()
            .users
            .push("u2".into());
        assert_eq!(
            route(
                &restricted,
                Some("b1"),
                &message(Some("g1"), "c1", "<@b1> hello")
            ),
            RouteDecision::Ignore
        );
        let mut bot = message(Some("g1"), "c1", "<@b1> hello");
        bot.author_is_bot = true;
        assert_eq!(route(&cfg, Some("b1"), &bot), RouteDecision::Ignore);
        let mut webhook = message(Some("g1"), "c1", "<@b1> hello");
        webhook.webhook_id = Some("hook");
        assert_eq!(route(&cfg, Some("b1"), &webhook), RouteDecision::Ignore);
    }

    #[test]
    fn disabled_and_distinct_channels_do_not_route_or_share_sessions() {
        let mut cfg = config();
        cfg.guilds
            .get_mut("g1")
            .unwrap()
            .channels
            .get_mut("c1")
            .unwrap()
            .enabled = false;
        assert_eq!(
            route(&cfg, Some("b1"), &message(Some("g1"), "c1", "<@b1> hello")),
            RouteDecision::Ignore
        );
        let mut cfg = config();
        cfg.guilds.get_mut("g1").unwrap().enabled = false;
        assert_eq!(
            route(&cfg, Some("b1"), &message(Some("g1"), "c1", "<@b1> hello")),
            RouteDecision::Ignore
        );
        let mut cfg = config();
        cfg.guilds
            .get_mut("g1")
            .unwrap()
            .channels
            .get_mut("c1")
            .unwrap()
            .users
            .push("u2".into());
        assert_eq!(
            route(&cfg, Some("b1"), &message(Some("g1"), "c1", "<@b1> hello")),
            RouteDecision::Ignore
        );
        let mut cfg = config();
        cfg.guilds
            .get_mut("g1")
            .unwrap()
            .channels
            .get_mut("c1")
            .unwrap()
            .require_mention = false;
        assert!(matches!(
            route(&cfg, Some("b1"), &message(Some("g1"), "c1", "hello")),
            RouteDecision::Dispatch { .. }
        ));
        let cfg = config();
        let first = route(&cfg, Some("b1"), &message(Some("g1"), "c1", "<@b1> hello"));
        let second = route(
            &cfg,
            Some("b1"),
            &message(Some("g1"), "thread", "<@b1> hello"),
        );
        assert_ne!(first, second);
    }

    #[test]
    fn attachment_only_message_bypasses_mention_gating() {
        assert_eq!(
            route(&config(), Some("b1"), &message(Some("g1"), "c1", "")),
            RouteDecision::Ignore
        );
        let mut captioned = message(Some("g1"), "c1", "a picture");
        captioned.has_attachments = true;
        assert_eq!(
            route(&config(), Some("b1"), &captioned),
            RouteDecision::Ignore
        );
        let mut attachment = message(Some("g1"), "c1", "");
        attachment.has_attachments = true;
        assert!(matches!(
            route(&config(), Some("b1"), &attachment),
            RouteDecision::Dispatch { text, .. } if text.is_empty()
        ));
    }
}
