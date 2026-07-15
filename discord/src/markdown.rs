//! Conservative agent-markdown rendering and Discord-sized chunking.

pub const MESSAGE_LIMIT: usize = 2_000;
const MAX_CARRIED_LANGUAGE: usize = MESSAGE_LIMIT - 9;

pub fn render(input: &str) -> String {
    let mut fenced = false;
    input
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("```") {
                fenced = !fenced;
                return line.to_owned();
            }
            if fenced {
                return line.to_owned();
            }
            let hashes = line.bytes().take_while(|b| *b == b'#').count();
            let line = if hashes > 0 && line.as_bytes().get(hashes) == Some(&b' ') {
                &line[hashes + 1..]
            } else {
                line
            };
            line.replace("@everyone", "@\u{200b}everyone")
                .replace("@here", "@\u{200b}here")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Split at newlines or whitespace where possible. Each emitted message has
/// balanced fences; a fence open at a split is closed then re-opened in the
/// following message with its language tag retained.
pub fn chunk(input: &str) -> Vec<String> {
    if input.is_empty() {
        return vec![String::new()];
    }
    let mut remaining = input;
    let mut output = Vec::new();
    let mut reopen: Option<String> = None;
    while !remaining.is_empty() {
        let continued_fence = reopen.is_some();
        let carried_language = reopen.take();
        let prefix = carried_language
            .as_ref()
            .map(|lang| format!("```{lang}\n"))
            .unwrap_or_default();
        let reserve = if prefix.is_empty() { 4 } else { 4 };
        let budget = MESSAGE_LIMIT
            .saturating_sub(prefix.chars().count() + reserve)
            .max(1);
        let take = remaining
            .chars()
            .take(budget)
            .map(char::len_utf8)
            .sum::<usize>();
        let mut end = if remaining.chars().count() <= budget {
            remaining.len()
        } else {
            take
        };
        if end < remaining.len() {
            let candidate = &remaining[..end];
            if let Some(boundary) = candidate
                .rfind('\n')
                .or_else(|| candidate.rfind(char::is_whitespace))
            {
                if boundary > 0 {
                    end = boundary
                        + usize::from(
                            candidate.as_bytes()[boundary] == b'\n'
                                || candidate.as_bytes()[boundary].is_ascii_whitespace(),
                        );
                }
            }
        }
        let body = &remaining[..end];
        let mut message = prefix;
        message.push_str(body);
        let fences = body
            .lines()
            .filter_map(|line| line.trim_start().strip_prefix("```"))
            .collect::<Vec<_>>();
        let was_open = continued_fence ^ (fences.len() % 2 == 1);
        if was_open {
            let language = fences
                .last()
                .map(|fence| fence.trim().chars().take(MAX_CARRIED_LANGUAGE).collect())
                .unwrap_or_else(|| carried_language.unwrap_or_default());
            message.push_str("\n```");
            reopen = Some(language);
        }
        output.push(message);
        remaining = &remaining[end..];
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunks_at_sensible_boundary() {
        let chunks = chunk(&("word ".repeat(500)));
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|c| c.chars().count() <= MESSAGE_LIMIT));
        assert!(chunks[0].ends_with(' '));
    }
    #[test]
    fn reopens_fences_across_chunks() {
        let chunks = chunk(&format!("```rust\n{}\n```", "x\n".repeat(1500)));
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|c| c.matches("```").count() % 2 == 0));
        assert!(chunks[1].starts_with("```rust\n"));
    }
    #[test]
    fn long_fence_language_stays_within_the_limit() {
        let chunks = chunk(&format!("```{}\n{}", "r".repeat(1_993), "x\n".repeat(20)));
        assert!(chunks
            .iter()
            .all(|chunk| chunk.chars().count() <= MESSAGE_LIMIT));
    }
    #[test]
    fn renders_discord_markdown() {
        assert_eq!(
            render("## **hi** `__code__` @everyone"),
            "**hi** `__code__` @\u{200b}everyone"
        );
    }
}
