/// Slack-safe, intentionally conservative Markdown conversion.
///
/// Removes Markdown heading markers only when followed by whitespace. Converts
/// common emphasis outside inline and fenced code, preserving unknown markup.
pub fn render(input: &str) -> String {
    let mut fenced = false;
    input
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("```") {
                fenced = !fenced;
                return line.to_owned();
            }
            let line = if fenced { line } else { strip_heading(line) };
            if fenced {
                line.to_owned()
            } else {
                render_inline(line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn strip_heading(line: &str) -> &str {
    let markers = line.bytes().take_while(|byte| *byte == b'#').count();
    if markers > 0 && line.as_bytes().get(markers) == Some(&b' ') {
        &line[markers + 1..]
    } else {
        line
    }
}

fn render_inline(line: &str) -> String {
    let mut output = String::new();
    for (index, part) in line.split('`').enumerate() {
        if index % 2 == 0 {
            output.push_str(&neutralize_slack_controls(
                &part.replace("**", "*").replace("__", "*"),
            ));
        } else {
            output.push('`');
            output.push_str(part);
            output.push('`');
        }
    }
    output
}

fn neutralize_slack_controls(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find('<') {
        output.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('>') else {
            output.push_str("&lt;");
            output.push_str(after);
            return neutralize_broadcasts(&output);
        };
        let token = &after[..end];
        if token.starts_with("http://")
            || token.starts_with("https://")
            || token.starts_with("mailto:")
        {
            output.push('<');
            output.push_str(token);
            output.push('>');
        } else {
            output.push_str("&lt;");
            output.push_str(token);
            output.push('>');
        }
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    neutralize_broadcasts(&output)
}

fn neutralize_broadcasts(input: &str) -> String {
    input
        .replace("@channel", "@\u{200b}channel")
        .replace("@here", "@\u{200b}here")
        .replace("@everyone", "@\u{200b}everyone")
}

/// Split text into UTF-8-valid chunks. Chunks normally fit `limit` bytes; a
/// single Unicode scalar wider than `limit` is emitted intact because splitting
/// it would produce invalid UTF-8.
pub fn chunk(input: &str, limit: usize) -> Vec<String> {
    assert!(limit > 0, "chunk limit must be nonzero");
    if input.len() <= limit {
        return vec![input.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut remaining = input;
    while !remaining.is_empty() {
        if remaining.len() <= limit {
            chunks.push(remaining.to_owned());
            break;
        }
        let mut end = limit;
        while end > 0 && !remaining.is_char_boundary(end) {
            end -= 1;
        }
        if end == 0 {
            let scalar = remaining.chars().next().expect("nonempty");
            let width = scalar.len_utf8();
            chunks.push(remaining[..width].to_owned());
            remaining = &remaining[width..];
            continue;
        }
        let boundary = remaining[..end]
            .rfind(char::is_whitespace)
            .filter(|position| *position > 0)
            .map(|position| position + 1)
            .unwrap_or(end);
        chunks.push(remaining[..boundary].to_owned());
        remaining = &remaining[boundary..];
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_channels_and_code_while_rendering_emphasis() {
        assert_eq!(
            render("## **hello** #channel\n`__literal__`\n```\n**code**\n```"),
            "*hello* #channel\n`__literal__`\n```\n**code**\n```"
        );
    }

    #[test]
    fn neutralizes_mentions_but_keeps_normal_links() {
        assert_eq!(
            render("<@U123> @channel <https://example.com|site>"),
            "&lt;@U123> @\u{200b}channel <https://example.com|site>"
        );
    }

    #[test]
    fn chunks_unicode_without_invalid_utf8() {
        let input = "hello 👋 world 你好 friend";
        let chunks = chunk(input, 10);
        assert!(chunks.iter().all(|part| part.len() <= 10));
        assert_eq!(chunks.concat(), input);
    }

    #[test]
    fn scalar_wider_than_limit_makes_progress() {
        assert_eq!(chunk("👋x", 1), vec!["👋", "x"]);
    }

    #[test]
    fn chunks_at_whitespace_when_possible() {
        assert_eq!(chunk("one two three", 8), vec!["one two ", "three"]);
    }
}
