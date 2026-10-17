//! Progress labels. Safe mode uses tool names only. Detailed (default) mode
//! adds the target: path, command or first identifying argument, lightly masked. Tool outputs never reach the board.

use serde_json::{Map, Value};

use crate::redact::redact;

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

/// `<shell tool>: Running: <command>` etc. Rendered `args` is a JSON object
/// in every Dar backend that has one; unparsable args give the safe label.
/// `None` for task bookkeeping tools.
pub fn detailed_tool_label(name: &str, args: &str) -> Option<String> {
    let label = tool_label(name)?;
    let parsed = serde_json::from_str::<Value>(args);
    if !is_plain_name(name) {
        // Codex: the name is the command; a plain-text args is the same command.
        let command = match parsed {
            Err(_) if !args.trim().is_empty() => args,
            _ => name,
        };
        return Some(running(command).unwrap_or(label));
    }
    let Ok(Value::Object(args)) = parsed else {
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
        let command = string_arg(&args, &["command", "cmd"]).unwrap_or_default();
        return Some(running(&command).unwrap_or(label));
    }
    Some(match string_arg(&args, ID_KEYS) {
        Some(value) => trim_text(&format!("{label}: {}", redact(&value)), 80),
        None => label,
    })
}

/// `Running: <first non-empty line>`, redacted and trimmed.
fn running(command: &str) -> Option<String> {
    let line = command.lines().find(|l| !l.trim().is_empty())?;
    Some(trim_text(&format!("Running: {}", redact(line)), 120))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn detailed(name: &str, args: Value) -> Option<String> {
        detailed_tool_label(name, &args.to_string())
    }

    #[test]
    fn safe_labels_unchanged() {
        assert_eq!(tool_label("read").unwrap(), "Reading files");
        assert_eq!(tool_label("write").unwrap(), "Editing files");
        assert_eq!(tool_label("bash").unwrap(), "Running commands");
        assert_eq!(tool_label("web_search").unwrap(), "Searching the web");
        assert_eq!(
            tool_label("slack.send_message").unwrap(),
            "Using send message"
        );
        assert_eq!(
            tool_label("git status --short").unwrap(),
            "Running commands"
        );
        assert_eq!(tool_label("task.checkpoint"), None);
    }

    #[test]
    fn file_labels() {
        assert_eq!(
            detailed("read", json!({"path": "/a/b/c/d/notes.md"})).unwrap(),
            "Reading …/c/d/notes.md"
        );
        assert_eq!(
            detailed("edit", json!({"file_path": "src/x.rs"})).unwrap(),
            "Editing src/x.rs"
        );
    }

    #[test]
    fn shell_labels_show_the_real_command() {
        assert_eq!(
            detailed(
                "bash",
                json!({"command": "\n cd /x &&  rg -n foo | head -5\nmore"})
            )
            .unwrap(),
            "Running: cd /x && rg -n foo | head -5"
        );
        let long = detailed("bash", json!({"command": "x".repeat(300)})).unwrap();
        assert_eq!(long.chars().count(), 120);
        assert!(long.ends_with('…'));
        // Codex: the name is the command.
        assert_eq!(
            detailed_tool_label("git log --oneline -3", "{}").unwrap(),
            "Running: git log --oneline -3"
        );
        assert_eq!(
            detailed_tool_label("curl -H 'Authorization: Bearer abc123' x", "").unwrap(),
            "Running: curl -H 'Authorization: Bearer •••' x"
        );
    }

    #[test]
    fn other_tools_show_first_arg() {
        assert_eq!(
            detailed("linear.get_issue", json!({"id": "ALG-1"})).unwrap(),
            "Using get issue: ALG-1"
        );
        assert_eq!(
            detailed("web_search", json!({"query": "rust regex"})).unwrap(),
            "Searching the web: rust regex"
        );
        assert_eq!(
            detailed("web_search", json!({})).unwrap(),
            "Searching the web"
        );
        assert_eq!(detailed("task.update", json!({"id": "x"})), None);
    }

    #[test]
    fn masking_bugs_fixed() {
        let label = |args: Value| detailed("web_search", args).unwrap();
        // (a) ASCII word boundaries after accented letters
        let out = label(json!({"query": "éghp_abcdefghij"}));
        assert!(!out.contains("ghp_"), "{out}");
        let out = label(json!({"query": "épassword=hunter2"}));
        assert!(!out.contains("hunter2"), "{out}");
        // (b) escaped quotes inside a JSON value
        let out = label(json!({"query": r#"{"password":"hello \"hunter2 world\""}"#}));
        assert!(!out.contains("hunter2"), "{out}");
        // (c) a value merely containing the mask is still masked
        let out = label(json!({"query": "password=•••hunter2"}));
        assert!(!out.contains("hunter2"), "{out}");
    }

    #[test]
    fn command_like_names_are_commands() {
        let out = detailed_tool_label("export GITHUB_TOKEN=abc123 && make", "{}").unwrap();
        assert_eq!(out, "Running: export GITHUB_TOKEN=••• && make");
    }
}
