//! IRC line splitter: strip basic markdown, then chunk a reply into IRC-safe
//! lines on word boundaries, respecting both a character budget and the 512-byte
//! protocol line limit, multibyte-safe. Pure; pacing is applied by the caller.

/// Soft character budget per line (well under the 512-byte protocol cap to leave
/// room for the `PRIVMSG <target> :` envelope and CRLF).
pub const MAX_CHARS: usize = 450;
/// Hard byte budget per chunk's text payload (conservative vs. the 512-byte line).
pub const MAX_BYTES: usize = 400;

/// Strip the markdown that most commonly leaks into plain-text IRC: bold/italic
/// emphasis markers, inline-code/backtick fences, heading markers, link syntax,
/// and angle-bracket autolinks. Conservative; leaves ordinary punctuation alone.
pub fn strip_markdown(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(text.len());
    let mut at_line_start = true;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        match c {
            '*' | '_' | '`' => {
                // Collapse runs of the same marker (e.g. `**`, ` ``` `).
                while i + 1 < n && chars[i + 1] == c {
                    i += 1;
                }
                at_line_start = false;
                i += 1;
            }
            '#' if at_line_start => {
                // Drop a leading run of `#` heading markers + following spaces.
                while i + 1 < n && chars[i + 1] == '#' {
                    i += 1;
                }
                while i + 1 < n && chars[i + 1] == ' ' {
                    i += 1;
                }
                // The heading prefix is consumed; the next char is line content.
                at_line_start = false;
                i += 1;
            }
            '[' => {
                // Try to match [text](url) — emit text, skip url+brackets.
                if let Some((text_part, skip)) = try_parse_link(&chars, i) {
                    out.push_str(&text_part);
                    i += skip;
                    at_line_start = false;
                } else {
                    out.push(c);
                    at_line_start = false;
                    i += 1;
                }
            }
            '<' => {
                // Try to match <url> autolink.
                if let Some((url, skip)) = try_parse_autolink(&chars, i) {
                    out.push_str(&url);
                    i += skip;
                    at_line_start = false;
                } else {
                    out.push(c);
                    at_line_start = false;
                    i += 1;
                }
            }
            '\n' => {
                out.push(c);
                at_line_start = true;
                i += 1;
            }
            _ => {
                out.push(c);
                at_line_start = false;
                i += 1;
            }
        }
    }
    out
}

/// Try to parse [text](url) starting at chars[i] (where chars[i] == '[').
/// Returns (text_content, chars_consumed) or None if not a valid link pattern.
fn try_parse_link(chars: &[char], i: usize) -> Option<(String, usize)> {
    // Find closing ]
    let mut j = i + 1;
    while j < chars.len() && chars[j] != ']' {
        j += 1;
    }
    if j >= chars.len() {
        return None;
    }
    // After ] must be (
    if j + 1 >= chars.len() || chars[j + 1] != '(' {
        return None;
    }
    // Find closing )
    let mut k = j + 2;
    while k < chars.len() && chars[k] != ')' {
        k += 1;
    }
    if k >= chars.len() {
        return None;
    }
    let text_part: String = chars[i + 1..j].iter().collect();
    Some((text_part, k - i + 1))
}

/// Try to parse <url> autolink starting at chars[i] (where chars[i] == '<').
/// Only strips if content starts with http:// or https://.
fn try_parse_autolink(chars: &[char], i: usize) -> Option<(String, usize)> {
    let mut j = i + 1;
    while j < chars.len() && chars[j] != '>' {
        j += 1;
    }
    if j >= chars.len() {
        return None;
    }
    let inner: String = chars[i + 1..j].iter().collect();
    if inner.starts_with("http://") || inner.starts_with("https://") {
        Some((inner, j - i + 1))
    } else {
        None
    }
}

/// Split a reply into IRC-safe chunks. Markdown is stripped first; the text is
/// then broken on word boundaries where possible, never exceeding [`MAX_CHARS`]
/// characters or [`MAX_BYTES`] bytes per chunk, and never splitting a UTF-8
/// codepoint. Empty/whitespace-only input yields no chunks.
pub fn split_message(text: &str, target: &str) -> Vec<String> {
    let stripped = strip_markdown(text);
    // "PRIVMSG " (8) + target + " :" (2) + CRLF (2) = overhead
    let envelope = 8 + target.len() + 4;
    let byte_budget = 512usize.saturating_sub(envelope).clamp(100, MAX_BYTES);
    let mut chunks = Vec::new();
    // Split into lines first so explicit newlines become separate IRC lines.
    for line in stripped.split('\n') {
        let line = line.trim_end();
        if line.trim().is_empty() {
            continue;
        }
        split_line(line, byte_budget, &mut chunks);
    }
    chunks
}

fn split_line(line: &str, max_bytes: usize, out: &mut Vec<String>) {
    let mut current = String::new();
    for word in line.split_whitespace() {
        if word_fits(&current, word, max_bytes) {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        } else {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            // A single word longer than the budget must be hard-split.
            if exceeds_budget(word, max_bytes) {
                hard_split(word, max_bytes, out);
            } else {
                current.push_str(word);
            }
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
}

/// Whether appending `word` (plus a separating space) to `current` stays within
/// both budgets.
fn word_fits(current: &str, word: &str, max_bytes: usize) -> bool {
    let sep = if current.is_empty() { 0 } else { 1 };
    let chars = current.chars().count() + sep + word.chars().count();
    let bytes = current.len() + sep + word.len();
    chars <= MAX_CHARS && bytes <= max_bytes
}

fn exceeds_budget(word: &str, max_bytes: usize) -> bool {
    word.chars().count() > MAX_CHARS || word.len() > max_bytes
}

/// Hard-split a single over-budget token on codepoint boundaries, respecting both
/// the char and byte budgets.
fn hard_split(word: &str, max_bytes: usize, out: &mut Vec<String>) {
    let mut current = String::new();
    let mut chars = 0usize;
    for ch in word.chars() {
        let next_bytes = current.len() + ch.len_utf8();
        let next_chars = chars + 1;
        if next_bytes > max_bytes || next_chars > MAX_CHARS {
            out.push(std::mem::take(&mut current));
            chars = 0;
        }
        current.push(ch);
        chars += 1;
    }
    if !current.is_empty() {
        out.push(current);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_line() {
        assert_eq!(split_message("hello there", ""), vec!["hello there".to_string()]);
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(split_message("", "").is_empty());
        assert!(split_message("   \n  ", "").is_empty());
    }

    #[test]
    fn markdown_is_stripped() {
        assert_eq!(strip_markdown("**bold** and _italic_"), "bold and italic");
        assert_eq!(strip_markdown("use `code` here"), "use code here");
        assert_eq!(strip_markdown("# Heading"), "Heading");
        assert_eq!(strip_markdown("```rust"), "rust");
    }

    #[test]
    fn markdown_links_are_stripped() {
        assert_eq!(strip_markdown("[see here](https://example.com)"), "see here");
        assert_eq!(strip_markdown("<https://example.com>"), "https://example.com");
    }

    #[test]
    fn mid_line_hash_is_preserved() {
        // IRC channel names keep their sigil.
        assert_eq!(strip_markdown("join #rust-lang"), "join #rust-lang");
        // `#` followed by a space mid-line must not merge adjacent tokens.
        assert_eq!(strip_markdown("C# language"), "C# language");
        // Heading marker is still stripped only at line start, including after
        // a newline.
        assert_eq!(strip_markdown("intro\n# Heading"), "intro\nHeading");
        // Leading whitespace before `#` means it is not a line-start heading.
        assert_eq!(strip_markdown(" # not-heading"), " # not-heading");
    }

    #[test]
    fn over_limit_splits_on_word_boundaries() {
        let word = "lorem";
        let many = vec![word; 200].join(" "); // ~1199 chars
        let chunks = split_message(&many, &"#".repeat(100));
        assert!(chunks.len() > 1);
        for c in &chunks {
            assert!(c.chars().count() <= MAX_CHARS);
            assert!(c.len() <= MAX_BYTES);
            // Word-boundary: no chunk should start or end mid-word with a space.
            assert!(!c.starts_with(' ') && !c.ends_with(' '));
        }
        // Round-trips back to the same words.
        let rejoined = chunks.join(" ");
        assert_eq!(rejoined.split_whitespace().count(), 200);
    }

    #[test]
    fn multibyte_is_codepoint_safe() {
        // Each emoji is 4 bytes; a long run forces byte-budget splitting.
        let s = "😀".repeat(200);
        let chunks = split_message(&s, &"#".repeat(100));
        assert!(chunks.len() > 1);
        for c in &chunks {
            assert!(c.len() <= MAX_BYTES);
            // Every chunk is valid UTF-8 made only of whole emoji.
            assert!(c.chars().all(|ch| ch == '😀'));
        }
        let total: usize = chunks.iter().map(|c| c.chars().count()).sum();
        assert_eq!(total, 200);
    }

    #[test]
    fn explicit_newlines_become_separate_lines() {
        let chunks = split_message("line one\nline two", "");
        assert_eq!(chunks, vec!["line one".to_string(), "line two".to_string()]);
    }

    #[test]
    fn long_single_word_is_hard_split() {
        let word = "a".repeat(MAX_CHARS + 50);
        let chunks = split_message(&word, "");
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(c.chars().count() <= MAX_CHARS);
        }
    }
}
