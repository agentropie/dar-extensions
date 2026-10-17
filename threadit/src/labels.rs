//! Safe progress labels from tool names. Arguments and outputs never reach the board.

/// Map a tool name to a short label. `None` for task bookkeeping tools.
pub fn tool_label(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    if lower.starts_with("task.") || lower.starts_with("task_") {
        return None;
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
        assert_eq!(tool_label("!!!").unwrap(), "Using a tool");
    }
}
