//! Mattermost markdown and conservative character-counted post splitting.
pub const MESSAGE_LIMIT: usize = 15_000;
const MAX_CARRIED_LANGUAGE: usize = MESSAGE_LIMIT - 9;
pub fn render(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let mut result = String::with_capacity(input.len());
    for (index, ch) in input.char_indices() {
        result.push(ch);
        if ch != '@' {
            continue;
        }
        let rest = &lower[index + 1..];
        if ["channel", "all", "here"].iter().any(|name| {
            rest.strip_prefix(name).is_some_and(|tail| {
                tail.chars()
                    .next()
                    .is_none_or(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
            })
        }) {
            result.push('\u{200b}');
        }
    }
    result
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
        let reserve = 4;
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
    fn renders_markdown_and_neutralizes_only_broadcast_mentions() {
        assert_eq!(
            render("# Hello @channel @ALL @here @helper @channel2"),
            "# Hello @\u{200b}channel @\u{200b}ALL @\u{200b}here @helper @channel2"
        );
        assert_eq!(chunk(""), vec![String::new()]);
    }
    #[test]
    fn chunk_limit_counts_characters_without_truncating_multibyte_text() {
        let input = "界".repeat(15_001);
        let posts = chunk(&input);
        assert_eq!(posts.len(), 2);
        assert!(posts.iter().all(|post| post.chars().count() <= 15_000));
        assert_eq!(posts.concat(), input);
        assert!(posts[0].len() > 15_000);
        let words = "word ".repeat(4_000);
        let posts = chunk(&words);
        assert!(posts[0].ends_with(' '));
        assert_eq!(posts.concat(), words);
    }
    #[test]
    fn chunked_code_posts_reopen_language_and_balance_fences() {
        let posts = chunk(&format!("```rust\n{}\n```", "x\n".repeat(10_000)));
        assert_eq!(posts.len(), 2);
        assert!(posts
            .iter()
            .all(|post| post.chars().count() <= 15_000 && post.matches("```").count() % 2 == 0));
        assert!(posts[1].starts_with("```rust\n"));
        let long_tag = chunk(&format!("```{}\n{}", "r".repeat(14_993), "x\n".repeat(20)));
        assert!(long_tag.iter().all(|post| post.chars().count() <= 15_000));
    }
}
