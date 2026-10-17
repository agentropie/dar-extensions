//! Progress labels. Safe mode uses tool names only. Detailed mode adds a
//! redacted target from the arguments. Tool outputs never reach the board.

use serde_json::{Map, Value};

use crate::redact::{looks_like_token, redact};

/// Map a tool name to a short label. `None` for task bookkeeping tools.
pub fn tool_label(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    if lower.starts_with("task.") || lower.starts_with("task_") {
        return None;
    }
    if !is_plain_name(name) {
        return Some(RUNNING.to_string());
    }
    let normalized = lower.replace(['.', '-'], "_");
    let exact = match normalized.as_str() {
        "read" => Some("Reading files"),
        "write" | "edit" => Some("Editing files"),
        "bash" => Some("Running commands"),
        "extract_document" => Some("Reading documents"),
        "describe_image" => Some("Looking at an image"),
        _ => None,
    };
    if let Some(label) = exact {
        return Some(label.to_string());
    }
    for (prefix, label) in [
        ("web_search", "Searching the web"),
        ("websearch", "Searching the web"),
        ("web_fetch", "Searching the web"),
        ("fetch", "Searching the web"),
        ("scheduler_", "Managing scheduled jobs"),
    ] {
        if normalized.starts_with(prefix) {
            return Some(label.to_string());
        }
    }
    Some(humanize(name))
}

const RUNNING: &str = "Running commands";

/// A tool identifier, not free text. Codex names shell calls by their raw
/// command, which must never be humanized into a label.
fn is_plain_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._:/-".contains(c))
}

fn humanize(name: &str) -> String {
    let rest = name.split_once('.').map(|(_, r)| r).unwrap_or(name);
    let words: String = rest
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let words: String = words.chars().take(40).collect();
    if words.is_empty() {
        "Using a tool".to_string()
    } else {
        format!("Using {words}")
    }
}

// ---- detailed mode ----

/// Collapse whitespace and trim to `max` characters with an ellipsis.
pub fn trim_text(text: &str, max: usize) -> String {
    let chars: Vec<char> = text
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .collect();
    if chars.len() <= max {
        chars.into_iter().collect()
    } else {
        let mut out: String = chars[..max - 1].iter().collect();
        out.push('…');
        out
    }
}

const PATH_KEYS: &[&str] = &["path", "file_path", "filePath", "file"];
const ID_KEYS: &[&str] = &[
    "id",
    "issue",
    "issueId",
    "query",
    "q",
    "title",
    "path",
    "url",
    "name",
    "thread_id",
    "threadId",
    "jobId",
    "job_id",
];
/// Shell tool names across Dar backends (pi, opencode: `bash`; others vary).
const SHELL_TOOLS: &[&str] = &["bash", "shell", "exec_command"];

fn string_arg(args: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| match args.get(*key)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

pub fn short_path(p: &str) -> String {
    let parts: Vec<&str> = p.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    if parts.len() > 3 {
        format!("…/{}", parts[parts.len() - 3..].join("/"))
    } else {
        p.to_string()
    }
}

/// `host/path` of a URL, without credentials, query or fragment.
fn short_url(raw: &str) -> String {
    let Some((scheme, rest)) = raw.split_once("://") else {
        return raw.to_string();
    };
    let valid_scheme = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+.-".contains(c));
    if !valid_scheme {
        return raw.to_string();
    }
    let rest = rest.split(['?', '#']).next().unwrap_or("");
    let (authority, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let host = authority.rsplit('@').next().unwrap_or("");
    if host.is_empty() {
        return raw.to_string();
    }
    let path = if path == "/" { "" } else { path };
    format!("{host}{path}")
}

/// Label with a redacted target from the rendered `args` (a JSON object in
/// every Dar backend that has one). Unparsable args give the safe label.
/// `None` for task bookkeeping tools.
pub fn detailed_tool_label(name: &str, args: &str) -> Option<String> {
    let label = tool_label(name)?;
    if !is_plain_name(name) {
        // Codex: the name is the command; a plain-text args is the same command.
        let command = match serde_json::from_str::<Value>(args) {
            Err(_) if !args.trim().is_empty() => args,
            _ => name,
        };
        let shown = bash_label(command);
        return Some(if shown.is_empty() {
            label
        } else {
            trim_text(&format!("Running: {}", redact(&shown)), 80)
        });
    }
    let Ok(Value::Object(args)) = serde_json::from_str::<Value>(args) else {
        return Some(label);
    };
    let lower = name.to_lowercase();
    let verb = match lower.as_str() {
        "read" => Some("Reading"),
        "write" => Some("Writing"),
        "edit" => Some("Editing"),
        _ => None,
    };
    if let Some(verb) = verb {
        return Some(match string_arg(&args, PATH_KEYS) {
            Some(p) => trim_text(&format!("{verb} {}", redact(&short_path(&p))), 120),
            None => label,
        });
    }
    if SHELL_TOOLS.contains(&lower.as_str()) {
        let command = match args.get("command").or_else(|| args.get("cmd")) {
            Some(Value::String(s)) => s.as_str(),
            _ => "",
        };
        let shown = bash_label(command);
        return Some(if shown.is_empty() {
            label
        } else {
            trim_text(&format!("Running: {}", redact(&shown)), 80)
        });
    }
    let Some((key, value)) = ID_KEYS
        .iter()
        .find_map(|k| string_arg(&args, &[k]).map(|v| (*k, v)))
    else {
        return Some(label);
    };
    let value = match key {
        "url" => short_url(&value),
        "path" => short_path(&value),
        _ => value,
    };
    Some(format!("{label}: {}", trim_text(&redact(&value), 60)))
}

const MAX_WORDS: usize = 6;

fn is_word(w: &str) -> bool {
    !w.is_empty()
        && w.chars()
            .all(|c| c.is_ascii_alphanumeric() || "._/~-".contains(c))
}

fn is_assignment(w: &str) -> bool {
    let Some((name, _)) = w.split_once('=') else {
        return false;
    };
    name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The visible part of a shell command: the program and its leading plain
/// words (no options), from the first non-empty line, without leading env
/// assignments or a plain `cd <dir> &&` prefix. Cut with " …" at the first
/// option, risky or token-like word. Empty when nothing is safe to show.
pub fn bash_label(command: &str) -> String {
    let line = command
        .split('\n')
        .find(|l| !l.trim().is_empty())
        .unwrap_or("");
    let words: Vec<&str> = line.split_whitespace().collect();
    let at = |i: usize| words.get(i).copied().unwrap_or("");
    let mut i = 0;
    while i < words.len() && is_assignment(words[i]) {
        // A quoted or escaped value may span words. Show nothing then.
        if words[i].contains(['"', '\'', '`', '\\', '$']) {
            return String::new();
        }
        i += 1;
    }
    // Skip `cd <plain-dir> &&` / `cd <plain-dir>;` prefixes. Anything else stays.
    while at(i) == "cd" {
        let target = at(i + 1);
        if target.ends_with(';') && is_word(&target[..target.len() - 1]) && !at(i + 2).is_empty() {
            i += 2;
        } else if is_word(target)
            && (at(i + 2) == "&&" || at(i + 2) == ";")
            && !at(i + 3).is_empty()
        {
            i += 3;
        } else {
            break;
        }
    }
    let mut shown = Vec::new();
    while i < words.len() && shown.len() < MAX_WORDS {
        let word = words[i];
        if word.starts_with('-') || !is_word(word) || looks_like_token(word) {
            break;
        }
        shown.push(word);
        i += 1;
    }
    if shown.is_empty() {
        String::new()
    } else if i < words.len() {
        format!("{} …", shown.join(" "))
    } else {
        shown.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_map() {
        assert_eq!(tool_label("read").unwrap(), "Reading files");
        assert_eq!(tool_label("Edit").unwrap(), "Editing files");
        assert_eq!(tool_label("write").unwrap(), "Editing files");
        assert_eq!(tool_label("bash").unwrap(), "Running commands");
        assert_eq!(tool_label("web_search").unwrap(), "Searching the web");
        assert_eq!(tool_label("fetch").unwrap(), "Searching the web");
        assert_eq!(
            tool_label("slack.send_message").unwrap(),
            "Using send message"
        );
        assert_eq!(tool_label("task.checkpoint"), None);
    }

    #[test]
    fn odd_names_stay_plain() {
        assert_eq!(tool_label("/etc/passwd").unwrap(), "Using etc passwd");
        assert_eq!(tool_label("!!!").unwrap(), "Running commands");
    }

    fn detailed(name: &str, args: Value) -> Option<String> {
        detailed_tool_label(name, &args.to_string())
    }

    fn bash(command: &str) -> String {
        detailed("bash", serde_json::json!({ "command": command })).unwrap()
    }

    /// Yoplai's checkpoint label: redacted and trimmed.
    fn progress(text: &str) -> String {
        trim_text(&redact(text), 200)
    }

    #[test]
    fn adds_file_paths_shortened() {
        use serde_json::json;
        assert_eq!(
            detailed("read", json!({"path": "memory.md"})).unwrap(),
            "Reading memory.md"
        );
        assert_eq!(
            detailed("edit", json!({"path": "src/labels.ts"})).unwrap(),
            "Editing src/labels.ts"
        );
        assert_eq!(
            detailed("write", json!({"file_path": "/a/b/c/d/e.ts"})).unwrap(),
            "Writing …/c/d/e.ts"
        );
        assert_eq!(
            detailed("edit", json!({"filePath": "/a/b/c/d/e.ts"})).unwrap(),
            "Editing …/c/d/e.ts"
        );
        assert_eq!(detailed("read", json!({})).unwrap(), "Reading files");
        assert_eq!(short_path("a/b/c"), "a/b/c");
    }

    #[test]
    fn unparsable_args_fall_back_to_safe_label() {
        assert_eq!(
            detailed_tool_label("read", "not json").unwrap(),
            "Reading files"
        );
        assert_eq!(
            detailed_tool_label("bash", "git status").unwrap(),
            "Running commands"
        );
        assert_eq!(detailed_tool_label("bash", "").unwrap(), "Running commands");
        assert_eq!(
            detailed_tool_label("slack.send", "\"nope\"").unwrap(),
            "Using send"
        );
    }

    #[test]
    fn shows_the_first_bash_line_trimmed_and_redacted() {
        assert_eq!(bash("\ngit status\nrm -rf /"), "Running: git status");
        let long = bash(&format!("echo {}", "x".repeat(200)));
        assert_eq!(long.chars().count(), 80);
        assert!(long.ends_with('…'));
        assert!(!bash("curl -H 'Authorization: Bearer abc123' x").contains("abc123"));
        assert_eq!(bash("API_KEY=hunter2 run"), "Running: run");
    }

    #[test]
    fn maps_other_shell_tool_names() {
        use serde_json::json;
        assert_eq!(
            detailed("Bash", json!({"command": "git status"})).unwrap(),
            "Running: git status"
        );
        assert_eq!(
            detailed("exec_command", json!({"cmd": "ls -la"})).unwrap(),
            "Running: ls …"
        );
        assert_eq!(
            detailed("shell", json!({"command": ["ls"]})).unwrap(),
            "Using shell"
        );
    }

    #[test]
    fn adds_one_identifying_argument_for_other_tools() {
        use serde_json::json;
        assert_eq!(
            detailed("linear.get_issue", json!({"id": "ALG-1", "body": "x"})).unwrap(),
            "Using get issue: ALG-1"
        );
        assert_eq!(
            detailed(
                "web_fetch",
                json!({"url": "https://u:p@ex.com/a/b?token=1"})
            )
            .unwrap(),
            "Searching the web: ex.com/a/b"
        );
        assert_eq!(
            detailed("web_search", json!({"query": "pnpm vitest"})).unwrap(),
            "Searching the web: pnpm vitest"
        );
        assert_eq!(
            detailed("scheduler.run", json!({"jobId": 42})).unwrap(),
            "Managing scheduled jobs: 42"
        );
        assert_eq!(
            detailed("slack.send", json!({"text": "hi"})).unwrap(),
            "Using send"
        );
        assert_eq!(detailed("task.checkpoint", json!({"id": "x"})), None);
        assert_eq!(detailed("task_update", json!({})), None);
        let long = detailed("web_search", json!({"q": "y".repeat(100)})).unwrap();
        assert_eq!(long.chars().count(), "Searching the web: ".len() + 60);
    }

    #[test]
    fn progress_redacts_and_trims() {
        assert_eq!(
            progress("Deployed with password=abc"),
            "Deployed with password=•••"
        );
        assert_eq!(progress(&"y".repeat(500)).chars().count(), 200);
        assert_eq!(trim_text("a  b\n c", 10), "a b c");
    }

    #[test]
    fn credential_forms_never_reach_a_label() {
        let rows = [
            (
                "curl -u alice:hunter2 https://example.com",
                "Running: curl …",
                "curl -u ••• https://example.com",
            ),
            (
                "SSHPASS=hunter2 sshpass -e ssh host",
                "Running: sshpass …",
                "SSHPASS=••• sshpass -e ••• host",
            ),
            (
                "MYSQL_PWD=hunter2 mysql -u root",
                "Running: mysql …",
                "MYSQL_PWD=••• mysql -u •••",
            ),
            (
                "PASSWORD=hello\\ world cmd",
                "Running commands",
                "PASSWORD=••• cmd",
            ),
            (
                "PASSWORD='hello''world' cmd",
                "Running commands",
                "PASSWORD=••• cmd",
            ),
            (
                r#"curl -H "Cookie: theme=dark; session=abc123; sid=xyz789" x"#,
                "Running: curl …",
                r#"curl -H "Cookie: •••" x"#,
            ),
            (
                r#"curl -d "{\"password\":\"hunter2\"}" x"#,
                "Running: curl …",
                r#"curl -d "{\"password\":\"•••\"}" x"#,
            ),
            (
                r#"curl -d '{"password":123456,"token":987654}' x"#,
                "Running: curl …",
                r#"curl -d '{"password":•••,"token":•••}' x"#,
            ),
            (
                r#"curl "https://example.com?sig=abc123""#,
                "Running: curl …",
                r#"curl "https://example.com?•••""#,
            ),
            (
                r#"curl -H "Authorization: Bearer abc123" x"#,
                "Running: curl …",
                r#"curl -H "Authorization: •••" x"#,
            ),
            (
                "git push https://ghp_abcdefghijklmno@github.com",
                "Running: git push …",
                "git push https://•••@github.com",
            ),
        ];
        let secrets = [
            "hunter2", "abc123", "xyz789", "123456", "987654", "world", "ghp_", "alice",
        ];
        for (command, expected_bash, expected_progress) in rows {
            let shown = bash(command);
            let thought = progress(command);
            assert_eq!(shown, expected_bash, "{command}");
            assert_eq!(thought, expected_progress, "{command}");
            assert_eq!(progress(&thought), thought, "idempotent: {command}");
            for secret in secrets {
                assert!(!shown.contains(secret), "{command}");
                assert!(!thought.contains(secret), "{command}");
            }
        }
    }

    #[test]
    fn keeps_harmless_commands_readable() {
        assert_eq!(bash("git log --author Alice"), "Running: git log …");
        assert_eq!(progress("git log --author Alice"), "git log --author Alice");
        assert_eq!(
            progress("monkey=banana keyboard x"),
            "monkey=banana keyboard x"
        );
        assert_eq!(bash("ls -la src | grep x"), "Running: ls …");
        assert_eq!(bash("FOO=bar pnpm test"), "Running: pnpm test");
    }

    #[test]
    fn skips_plain_cd_prefixes() {
        assert_eq!(bash("cd ~/pom && rg -n THREADIT .env"), "Running: rg …");
        assert_eq!(bash("cd /a; cd b && git log -5"), "Running: git log …");
        assert_eq!(
            bash("cd src ; tail -n 20 x.log | grep err"),
            "Running: tail …"
        );
        assert_eq!(bash("cd app && curl -u bob:pw host"), "Running: curl …");
        assert_eq!(bash("cd ~/pom"), "Running: cd ~/pom");
    }

    #[test]
    fn shows_only_the_program_and_leading_plain_words() {
        let rows = [
            ("curl -sd hunter2 x", "Running: curl …"),
            ("cd /tmp && curl -sd hunter2 x", "Running: curl …"),
            ("mysql -pab db", "Running: mysql …"),
            ("cd /tmp && mysql -pab db", "Running: mysql …"),
            ("mysql -phunter2 db", "Running: mysql …"),
            ("curl -ualice:hunter2 x", "Running: curl …"),
            (r#"curl -H"X-Custom: hunter2" x"#, "Running: curl …"),
            ("curl -dopaquevalue x", "Running: curl …"),
            (
                "yoplai subagents status sar_x --json",
                "Running: yoplai subagents status sar_x …",
            ),
            ("a b c d e f g h", "Running: a b c d e f …"),
            ("git status", "Running: git status"),
            (r#"cd "a; hunter2 other" && git status"#, "Running: cd …"),
            ("cd 'my dir' && git status", "Running: cd …"),
            ("cd a\\;b && git status", "Running: cd …"),
            (r#"cd x "&&" hunter2"#, "Running: cd x …"),
        ];
        for (command, expected) in rows {
            let shown = bash(command);
            assert_eq!(shown, expected, "{command}");
            for secret in ["hunter2", "ab db", "opaque", "alice"] {
                assert!(!shown.contains(secret), "{command}");
            }
        }
    }

    #[test]
    fn codex_command_names_never_humanized() {
        let cmd = "curl -u alice:hunter2 x";
        assert_eq!(tool_label(cmd).unwrap(), "Running commands");
        assert_eq!(detailed_tool_label(cmd, cmd).unwrap(), "Running: curl …");
        assert_eq!(detailed_tool_label(cmd, "").unwrap(), "Running: curl …");
        assert_eq!(
            detailed_tool_label("git status", "git status").unwrap(),
            "Running: git status"
        );
        assert_eq!(
            detailed_tool_label("PASSWORD='a b' cmd", "PASSWORD='a b' cmd").unwrap(),
            "Running commands"
        );
        let long = "x".repeat(65);
        assert_eq!(tool_label(&long).unwrap(), "Running commands");
        for label in [
            tool_label(cmd).unwrap(),
            detailed_tool_label(cmd, cmd).unwrap(),
        ] {
            assert!(!label.contains("alice") && !label.contains("hunter2"));
        }
    }

    #[test]
    fn codex_non_shell_tools_keep_labels() {
        // mcpToolCall is named `server.tool`; others use their tool name.
        assert_eq!(tool_label("linear.get_issue").unwrap(), "Using get issue");
        assert_eq!(
            detailed_tool_label("linear.get_issue", r#"{"id":"ALG-1"}"#).unwrap(),
            "Using get issue: ALG-1"
        );
        assert_eq!(tool_label("apply_patch").unwrap(), "Using apply patch");
        assert_eq!(tool_label("src/main.rs").unwrap(), "Using rs");
    }
}
