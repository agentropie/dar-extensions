use crate::{
    config::{MattermostConfig, ThreadPolicy},
    session::SessionKey,
};
#[derive(Clone, Debug)]
pub struct BotIdentity {
    pub id: String,
    pub username: String,
}
#[derive(Debug)]
pub struct InboundPost<'a> {
    pub id: &'a str,
    pub channel_id: &'a str,
    pub channel_type: &'a str,
    pub root_id: &'a str,
    pub author_id: &'a str,
    pub author_username: &'a str,
    pub text: &'a str,
    pub post_type: &'a str,
    pub from_bot: bool,
    pub from_webhook: bool,
    pub from_plugin: bool,
    pub has_files: bool,
    pub thread_engaged: bool,
    pub mentions: &'a [String],
}
#[derive(Debug, PartialEq, Eq)]
pub enum RouteDecision {
    Ignore,
    Dispatch {
        text: String,
        conversation: SessionKey,
        reply_root: Option<String>,
    },
}
pub fn route(
    config: &MattermostConfig,
    bot: &BotIdentity,
    post: &InboundPost<'_>,
) -> RouteDecision {
    if post.id.is_empty()
        || post.channel_id.is_empty()
        || post.author_id.is_empty()
        || post.author_id == bot.id
        || !post.post_type.is_empty()
        || post.from_webhook
        || post.from_plugin
    {
        return RouteDecision::Ignore;
    }
    let is_dm = post.channel_type == "D";
    let (text_mention, text) = strip_mention(post.text, &bot.username);
    let mentioned = text_mention || post.mentions.iter().any(|id| id == &bot.id);
    if post.from_bot && (!config.allow_bots.allows(post.author_id) || !mentioned) {
        return RouteDecision::Ignore;
    }
    let policy = if is_dm {
        if !config.dm.enabled
            || !crate::config::allowed(&config.dm.users, post.author_id, post.author_username)
        {
            return RouteDecision::Ignore;
        }
        config.dm.thread_policy
    } else {
        if !matches!(post.channel_type, "O" | "P" | "G") {
            return RouteDecision::Ignore;
        }
        let default = crate::config::ChannelConfig::default();
        let channel = match &config.channels {
            Some(channels) => match channels.get(post.channel_id) {
                Some(channel) => channel,
                None => return RouteDecision::Ignore,
            },
            None => &default,
        };
        if !crate::config::allowed(&channel.users, post.author_id, post.author_username)
            || (channel.require_mention
                && !mentioned
                && !(post.text.trim().is_empty() && post.has_files)
                && !(!post.root_id.is_empty() && post.thread_engaged))
        {
            return RouteDecision::Ignore;
        }
        channel.thread_policy
    };
    if text.is_empty() && !post.has_files {
        return RouteDecision::Ignore;
    }
    let reply_root = match policy {
        ThreadPolicy::Always => Some(
            if post.root_id.is_empty() {
                post.id
            } else {
                post.root_id
            }
            .to_owned(),
        ),
        ThreadPolicy::Never => None,
        ThreadPolicy::Follow => (!post.root_id.is_empty()).then(|| post.root_id.to_owned()),
    };
    let conversation = if !post.root_id.is_empty() {
        SessionKey::thread(is_dm, post.channel_id, post.root_id)
    } else if let Some(root) = &reply_root {
        SessionKey::thread(is_dm, post.channel_id, root)
    } else if is_dm {
        SessionKey::dm(post.channel_id)
    } else {
        SessionKey::channel(post.channel_id)
    };
    RouteDecision::Dispatch {
        text,
        conversation,
        reply_root,
    }
}

fn username_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
fn strip_mention(text: &str, username: &str) -> (bool, String) {
    if username.is_empty()
        || ["channel", "all", "here"]
            .iter()
            .any(|name| username.eq_ignore_ascii_case(name))
    {
        return (false, text.trim().to_owned());
    }
    let mention = format!("@{username}").to_ascii_lowercase();
    let lower = text.to_ascii_lowercase();
    let mut output = String::new();
    let mut cursor = 0;
    let mut found = false;
    for (start, _) in lower.match_indices(&mention) {
        let end = start + mention.len();
        if text[..start]
            .chars()
            .next_back()
            .is_some_and(|c| username_char(c) || c == '@')
            || text[end..].chars().next().is_some_and(username_char)
        {
            continue;
        }
        output.push_str(&text[cursor..start]);
        cursor = end;
        found = true;
    }
    output.push_str(&text[cursor..]);
    (found, output.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bot() -> BotIdentity {
        BotIdentity {
            id: "bbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            username: "helper".into(),
        }
    }
    fn post(text: &str) -> InboundPost<'_> {
        InboundPost {
            id: "pppppppppppppppppppppppppp",
            channel_id: "cccccccccccccccccccccccccc",
            channel_type: "O",
            root_id: "",
            author_id: "uuuuuuuuuuuuuuuuuuuuuuuuuu",
            author_username: "alice",
            text,
            post_type: "",
            from_bot: false,
            from_webhook: false,
            from_plugin: false,
            has_files: false,
            thread_engaged: false,
            mentions: &[],
        }
    }
    #[test]
    fn channel_mentions_open_one_shared_thread_conversation_but_dms_follow() {
        let cfg = MattermostConfig::default();
        assert_eq!(route(&cfg, &bot(), &post("hello")), RouteDecision::Ignore);
        assert_eq!(
            route(&cfg, &bot(), &post("@helper hello")),
            RouteDecision::Dispatch {
                text: "hello".into(),
                conversation: SessionKey::thread(
                    false,
                    "cccccccccccccccccccccccccc",
                    "pppppppppppppppppppppppppp"
                ),
                reply_root: Some("pppppppppppppppppppppppppp".into())
            }
        );
        let mut dm = post("hello");
        dm.channel_type = "D";
        assert_eq!(
            route(&cfg, &bot(), &dm),
            RouteDecision::Dispatch {
                text: "hello".into(),
                conversation: SessionKey::dm("cccccccccccccccccccccccccc"),
                reply_root: None
            }
        );
        dm.root_id = "tttttttttttttttttttttttttt";
        assert_eq!(
            route(&cfg, &bot(), &dm),
            RouteDecision::Dispatch {
                text: "hello".into(),
                conversation: SessionKey::thread(
                    true,
                    "cccccccccccccccccccccccccc",
                    "tttttttttttttttttttttttttt"
                ),
                reply_root: Some("tttttttttttttttttttttttttt".into())
            }
        );
    }
    #[test]
    fn dms_obey_enabled_and_id_or_username_allowlist() {
        let mut cfg = MattermostConfig::default();
        let mut dm = post("hello");
        dm.channel_type = "D";
        cfg.dm.enabled = false;
        assert_eq!(route(&cfg, &bot(), &dm), RouteDecision::Ignore);
        cfg.dm.enabled = true;
        for (user, accepted) in [
            ("nobody", false),
            ("uuuuuuuuuuuuuuuuuuuuuuuuuu", true),
            ("@ALICE", true),
        ] {
            cfg.dm.users = vec![user.into()];
            assert_eq!(
                matches!(route(&cfg, &bot(), &dm), RouteDecision::Dispatch { .. }),
                accepted
            );
        }
    }

    #[test]
    fn channels_use_optional_allowlist_and_per_channel_users() {
        let mut cfg = MattermostConfig::default();
        let input = post("@helper hello");
        for channel_type in ["O", "P", "G"] {
            let mut input = post("@helper hello");
            input.channel_type = channel_type;
            assert!(matches!(
                route(&cfg, &bot(), &input),
                RouteDecision::Dispatch { .. }
            ));
        }
        cfg.channels = Some(std::collections::HashMap::new());
        assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
        cfg.channels.as_mut().unwrap().insert(
            input.channel_id.into(),
            crate::config::ChannelConfig::default(),
        );
        for (user, accepted) in [
            ("nobody", false),
            ("uuuuuuuuuuuuuuuuuuuuuuuuuu", true),
            ("@ALICE", true),
        ] {
            cfg.channels
                .as_mut()
                .unwrap()
                .get_mut(input.channel_id)
                .unwrap()
                .users = vec![user.into()];
            assert_eq!(
                matches!(route(&cfg, &bot(), &input), RouteDecision::Dispatch { .. }),
                accepted
            );
        }
        cfg.channels
            .as_mut()
            .unwrap()
            .get_mut(input.channel_id)
            .unwrap()
            .require_mention = false;
        assert!(
            matches!(route(&cfg,&bot(),&post("plain hello")),RouteDecision::Dispatch{text,..} if text=="plain hello")
        );
    }

    #[test]
    fn mentions_match_ids_or_case_insensitive_username_boundaries() {
        let cfg = MattermostConfig::default();
        for (text, expected) in [
            ("@HELPER hello", Some("hello")),
            ("(@helper), hello", Some("(), hello")),
            ("@helper2 hello", None),
            ("@helper.foo hello", None),
            ("@helper-other hello", None),
            ("mail@helper hello", None),
            ("@channel hello", None),
            ("@all hello", None),
            ("@here hello", None),
            ("hello @helper!", Some("hello !")),
        ] {
            match expected {
                Some(expected) => assert!(
                    matches!(route(&cfg,&bot(),&post(text)),RouteDecision::Dispatch{text,..} if text==expected)
                ),
                None => assert_eq!(route(&cfg, &bot(), &post(text)), RouteDecision::Ignore),
            }
        }
        let mentions = vec![bot().id];
        let mut input = post("via event");
        input.mentions = &mentions;
        assert!(
            matches!(route(&cfg,&bot(),&input),RouteDecision::Dispatch{text,..} if text=="via event")
        );
        input.mentions = &[];
        assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
    }

    #[test]
    fn self_system_webhook_plugin_and_missing_identity_are_always_ignored() {
        let cfg = MattermostConfig {
            allow_bots: crate::config::AllowBots::All(true),
            ..Default::default()
        };
        for case in [
            "self",
            "system",
            "webhook",
            "plugin",
            "post-id",
            "channel-id",
            "author-id",
        ] {
            let mut input = post("@helper hello");
            match case {
                "self" => input.author_id = "bbbbbbbbbbbbbbbbbbbbbbbbbb",
                "system" => input.post_type = "system_join",
                "webhook" => input.from_webhook = true,
                "plugin" => input.from_plugin = true,
                "post-id" => input.id = "",
                "channel-id" => input.channel_id = "",
                _ => input.author_id = "",
            }
            assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore, "{case}");
        }
    }

    #[test]
    fn bot_allowlist_never_removes_the_mention_requirement_even_in_dms() {
        for channel_type in ["D", "O"] {
            let mut cfg = MattermostConfig::default();
            let mut input = post("@helper hello");
            input.from_bot = true;
            input.channel_type = channel_type;
            assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
            for (allow, accepted) in [
                (crate::config::AllowBots::All(true), true),
                (
                    crate::config::AllowBots::Only(vec![input.author_id.into()]),
                    true,
                ),
                (
                    crate::config::AllowBots::Only(vec!["oooooooooooooooooooooooooo".into()]),
                    false,
                ),
            ] {
                cfg.allow_bots = allow;
                assert_eq!(
                    matches!(route(&cfg, &bot(), &input), RouteDecision::Dispatch { .. }),
                    accepted
                );
            }
            cfg.allow_bots = crate::config::AllowBots::All(true);
            cfg.channels = Some(std::collections::HashMap::from([(
                input.channel_id.into(),
                crate::config::ChannelConfig {
                    require_mention: false,
                    ..Default::default()
                },
            )]));
            input.text = "hello";
            input.thread_engaged = true;
            input.root_id = "tttttttttttttttttttttttttt";
            assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
            input.text = "";
            input.has_files = true;
            assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
        }
    }

    #[test]
    fn empty_posts_are_ignored_but_attachment_only_posts_are_accepted() {
        let cfg = MattermostConfig::default();
        for channel_type in ["D", "O"] {
            for text in ["", "@helper", "@helper   "] {
                let mut input = post(text);
                input.channel_type = channel_type;
                assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
            }
            let mut input = post("");
            input.channel_type = channel_type;
            input.has_files = true;
            assert!(
                matches!(route(&cfg,&bot(),&input),RouteDecision::Dispatch{text,..} if text.is_empty())
            );
        }
        let mut captioned = post("caption without mention");
        captioned.has_files = true;
        assert_eq!(route(&cfg, &bot(), &captioned), RouteDecision::Ignore);
    }

    #[test]
    fn engaged_threads_accept_followups_in_the_same_conversation() {
        let mut cfg = MattermostConfig::default();
        let mut input = post("follow up");
        input.root_id = "pppppppppppppppppppppppppp";
        assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
        input.thread_engaged = true;
        assert!(
            matches!(route(&cfg,&bot(),&input),RouteDecision::Dispatch{conversation,..} if conversation==SessionKey::thread(false,input.channel_id,input.root_id))
        );
        cfg.channels = Some(std::collections::HashMap::from([(
            input.channel_id.into(),
            crate::config::ChannelConfig {
                users: vec!["oooooooooooooooooooooooooo".into()],
                ..Default::default()
            },
        )]));
        assert_eq!(route(&cfg, &bot(), &input), RouteDecision::Ignore);
    }

    #[test]
    fn thread_policies_preserve_input_topic_even_when_reply_is_flat() {
        for (kind, root, policy, key, target) in [
            (
                "D",
                "",
                ThreadPolicy::Always,
                "dm:cccccccccccccccccccccccccc:thread:pppppppppppppppppppppppppp",
                Some("pppppppppppppppppppppppppp"),
            ),
            (
                "D",
                "",
                ThreadPolicy::Never,
                "dm:cccccccccccccccccccccccccc",
                None,
            ),
            (
                "D",
                "",
                ThreadPolicy::Follow,
                "dm:cccccccccccccccccccccccccc",
                None,
            ),
            (
                "D",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Always,
                "dm:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                Some("tttttttttttttttttttttttttt"),
            ),
            (
                "D",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Never,
                "dm:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                None,
            ),
            (
                "D",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Follow,
                "dm:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                Some("tttttttttttttttttttttttttt"),
            ),
            (
                "O",
                "",
                ThreadPolicy::Always,
                "channel:cccccccccccccccccccccccccc:thread:pppppppppppppppppppppppppp",
                Some("pppppppppppppppppppppppppp"),
            ),
            (
                "O",
                "",
                ThreadPolicy::Never,
                "channel:cccccccccccccccccccccccccc:root",
                None,
            ),
            (
                "O",
                "",
                ThreadPolicy::Follow,
                "channel:cccccccccccccccccccccccccc:root",
                None,
            ),
            (
                "O",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Always,
                "channel:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                Some("tttttttttttttttttttttttttt"),
            ),
            (
                "O",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Never,
                "channel:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                None,
            ),
            (
                "O",
                "tttttttttttttttttttttttttt",
                ThreadPolicy::Follow,
                "channel:cccccccccccccccccccccccccc:thread:tttttttttttttttttttttttttt",
                Some("tttttttttttttttttttttttttt"),
            ),
        ] {
            let mut cfg = MattermostConfig::default();
            cfg.dm.thread_policy = policy;
            cfg.channels = Some(std::collections::HashMap::from([(
                "cccccccccccccccccccccccccc".into(),
                crate::config::ChannelConfig {
                    thread_policy: policy,
                    ..Default::default()
                },
            )]));
            let mut input = post("@helper hello");
            input.channel_type = kind;
            input.root_id = root;
            assert_eq!(
                route(&cfg, &bot(), &input),
                RouteDecision::Dispatch {
                    text: "hello".into(),
                    conversation: SessionKey::new(key.into()),
                    reply_root: target.map(str::to_owned)
                }
            );
        }
    }
}
