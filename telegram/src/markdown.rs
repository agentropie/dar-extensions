//! Pure markdown → Telegram converter.
//!
//! Agents reply in GitHub-flavoured markdown, but Telegram renders raw text
//! literally: `**bold**`, backticks, code fences and `[text](url)` links all
//! show as noise. This module converts that markdown into Telegram's
//! **MarkdownV2** dialect so replies render as real formatting.
//!
//! The whole design is built around one non-negotiable rule: **the message is
//! never lost.** Telegram rejects a message outright if its formatting markup
//! is malformed (a single stray unescaped char = parse error = nothing
//! delivered). So conversion feeds a fallback chain owned by the caller —
//! MarkdownV2 → plain text — and this module's job is to (a) produce correct,
//! fully-escaped MarkdownV2, and (b) split long replies into chunks that each
//! parse on their own.
//!
//! The module is pure (no I/O), so every rule below is exhaustively unit
//! testable via plain input → output assertions. The conversion rules mirror
//! the NousResearch `hermes-agent` Telegram adapter (`_escape_mdv2`,
//! `format_message`, table-wrapping helpers), reimplemented in Rust.

/// Telegram caps a single message at 4096 UTF-16 code units.
pub const TELEGRAM_MAX_UTF16: usize = 4096;

/// A table wider/taller than this many data rows is rendered as a fenced code
/// block rather than flattened into bullet groups.
const TABLE_ROW_BULLET_LIMIT: usize = 8;

/// How a converted chunk should be sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseMode {
    /// Send with `parse_mode: "MarkdownV2"`.
    MarkdownV2,
    /// Send as plain text with no `parse_mode`. The guaranteed-safe floor.
    Plain,
}

impl ParseMode {
    /// The value to put in the `parse_mode` field, or `None` for plain text.
    pub fn as_api_value(self) -> Option<&'static str> {
        match self {
            ParseMode::MarkdownV2 => Some("MarkdownV2"),
            ParseMode::Plain => None,
        }
    }
}

/// One ready-to-send chunk: the body plus the mode it must be sent with, and
/// the raw source span it was rendered from. `source` lets the caller drop this
/// exact span to a lossless plain-text send if Telegram rejects the markup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub text: String,
    pub parse_mode: ParseMode,
    /// The raw markdown source this chunk was rendered from (without the
    /// `(N/M)` indicator). Empty for plain chunks built directly from source.
    pub source: String,
}

/// The MarkdownV2 special characters that must be backslash-escaped in normal
/// (non-code, non-link) text. Order is irrelevant; membership is what matters.
const MDV2_SPECIAL: &[char] = &[
    '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!', '\\',
];

/// Escape a run of literal text for MarkdownV2 body context.
fn escape_mdv2(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if MDV2_SPECIAL.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Escape the contents of a code span/block. Inside code, MarkdownV2 only
/// treats `` ` `` and `\` as special.
fn escape_code(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch == '`' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Escape the URL inside a link. Inside a `(...)` link target, only `)` and
/// `\` are special.
fn escape_link_url(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    for ch in url.chars() {
        if ch == ')' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Count UTF-16 code units in a string (Telegram measures its 4096 limit in
/// UTF-16 units, so a `char` count over-counts astral emoji).
fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

/// Convert an agent's raw markdown reply into MarkdownV2 body text.
///
/// This handles block structure line-by-line (fenced code, tables, headings,
/// blockquotes) and inline spans within a line (bold, italic, strike, code,
/// links, spoilers). The output is fully escaped MarkdownV2.
pub fn to_markdown_v2(markdown: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let lines: Vec<&str> = markdown.split('\n').collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim_start();

        // Fenced code block: ``` or ~~~ (optionally with a language tag).
        if let Some(fence) = fence_marker(trimmed) {
            let lang = trimmed[fence.len()..].trim();
            let mut body: Vec<&str> = Vec::new();
            i += 1;
            while i < lines.len() {
                let inner = lines[i].trim_start();
                if fence_marker(inner).map(|f| f == fence).unwrap_or(false) {
                    i += 1;
                    break;
                }
                body.push(lines[i]);
                i += 1;
            }
            out.push(render_code_block(lang, &body));
            continue;
        }

        // GFM pipe table: a header row followed by a delimiter row.
        if is_table_row(line) && i + 1 < lines.len() && is_table_delimiter(lines[i + 1]) {
            let mut rows: Vec<&str> = vec![lines[i]];
            i += 1; // header
            let delimiter = lines[i];
            i += 1; // delimiter
            while i < lines.len() && is_table_row(lines[i]) {
                if !lines[i].trim().is_empty() {
                    rows.push(lines[i]);
                }
                i += 1;
            }
            out.push(render_table(rows[0], delimiter, &rows[1..]));
            continue;
        }

        out.push(render_line(line));
        i += 1;
    }
    out.join("\n")
}

/// Return the fence marker (```` ``` ```` or `~~~`) if `line` opens/closes a
/// fenced code block.
fn fence_marker(line: &str) -> Option<&'static str> {
    if line.starts_with("```") {
        Some("```")
    } else if line.starts_with("~~~") {
        Some("~~~")
    } else {
        None
    }
}

/// Render a fenced code block as a MarkdownV2 pre block, preserving language.
fn render_code_block(lang: &str, body: &[&str]) -> String {
    let inner = escape_code(&body.join("\n"));
    if lang.is_empty() {
        format!("```\n{inner}\n```")
    } else {
        // The language tag is not escaped; it must be a bare identifier.
        let lang = lang.split_whitespace().next().unwrap_or("");
        format!("```{lang}\n{inner}\n```")
    }
}

/// Render a single non-fenced, non-table line: detect block prefix (heading,
/// blockquote, list marker) then convert inline spans in the remainder.
fn render_line(line: &str) -> String {
    let indent_len = line.len() - line.trim_start().len();
    let indent = &line[..indent_len];
    let content = &line[indent_len..];

    // ATX heading `# ...` through `###### ...` → bold line.
    if let Some(rest) = heading_text(content) {
        return format!("{indent}*{}*", render_inline(rest));
    }

    // Blockquote `> ...` → MarkdownV2 blockquote (the `>` prefix is literal).
    if let Some(rest) = content.strip_prefix("> ") {
        return format!("{indent}>{}", render_inline(rest));
    }
    if content == ">" {
        return format!("{indent}>");
    }

    // Task list `- [ ]` / `- [x]` → checkbox glyph + rendered remainder.
    if let Some((marker, rest)) = task_list_item(content) {
        return format!("{indent}{} {}", escape_mdv2(marker), render_inline(rest));
    }

    // Bullet list `- ` / `* ` / `+ ` → a real bullet, escaped.
    if let Some(rest) = bullet_item(content) {
        return format!("{indent}\\- {}", render_inline(rest));
    }

    format!("{indent}{}", render_inline(content))
}

/// If `content` is an ATX heading, return the heading text (without `#`s).
fn heading_text(content: &str) -> Option<&str> {
    let hashes = content.chars().take_while(|&c| c == '#').count();
    if (1..=6).contains(&hashes) {
        let rest = &content[hashes..];
        if let Some(text) = rest.strip_prefix(' ') {
            return Some(text.trim_end());
        }
        if rest.is_empty() {
            return Some("");
        }
    }
    None
}

/// If `content` is a task-list item, return the checkbox glyph and remainder.
fn task_list_item(content: &str) -> Option<(&'static str, &str)> {
    for marker in ["- ", "* ", "+ "] {
        if let Some(rest) = content.strip_prefix(marker) {
            if let Some(after) = rest.strip_prefix("[ ] ") {
                return Some(("☐", after));
            }
            if let Some(after) = rest
                .strip_prefix("[x] ")
                .or_else(|| rest.strip_prefix("[X] "))
            {
                return Some(("☑", after));
            }
        }
    }
    None
}

/// If `content` is an unordered list item, return the remainder after the
/// marker.
fn bullet_item(content: &str) -> Option<&str> {
    for marker in ["- ", "* ", "+ "] {
        if let Some(rest) = content.strip_prefix(marker) {
            return Some(rest);
        }
    }
    None
}

/// Which inline entity types are currently open. MarkdownV2 (like Telegram)
/// forbids nesting an entity inside another of the *same* type, so a delimiter
/// whose type is already active is emitted literally instead of opening an
/// illegal nested span (which Telegram would reject).
#[derive(Clone, Copy, Default)]
struct Active {
    bold: bool,
    italic: bool,
    strike: bool,
    spoiler: bool,
}

/// Convert inline markdown spans within a single line into MarkdownV2, leaving
/// everything else escaped. Handles (in scan order): inline code, links,
/// images, bold, strike, spoiler, italic.
fn render_inline(text: &str) -> String {
    render_inline_ctx(text, Active::default())
}

fn render_inline_ctx(text: &str, active: Active) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            // Inline code `...` — protected region, escape only `` ` ``/`\`.
            '`' => {
                if let Some((code, next)) = take_inline_code(&chars, i) {
                    out.push('`');
                    out.push_str(&escape_code(&code));
                    out.push('`');
                    i = next;
                    continue;
                }
                out.push_str("\\`");
                i += 1;
            }
            // Image `![alt](url)` — render as a link to the url.
            '!' if i + 1 < chars.len() && chars[i + 1] == '[' => {
                if let Some((label, url, next)) = take_link(&chars, i + 1) {
                    out.push_str(&render_link(&label, &url, active));
                    i = next;
                    continue;
                }
                out.push_str("\\!");
                i += 1;
            }
            // Link `[text](url)`.
            '[' => {
                if let Some((label, url, next)) = take_link(&chars, i) {
                    out.push_str(&render_link(&label, &url, active));
                    i = next;
                    continue;
                }
                out.push_str("\\[");
                i += 1;
            }
            // Bold `**...**` or `__...__` → `*...*`.
            '*' | '_' if is_double(&chars, i, c) && !active.bold => {
                if let Some((inner, next)) = take_delimited(&chars, i, c, 2) {
                    let mut inner_active = active;
                    inner_active.bold = true;
                    out.push('*');
                    out.push_str(&render_inline_ctx(&inner, inner_active));
                    out.push('*');
                    i = next;
                    continue;
                }
                out.push('\\');
                out.push(c);
                i += 1;
            }
            // Strikethrough `~~...~~` → `~...~`.
            '~' if is_double(&chars, i, '~') && !active.strike => {
                if let Some((inner, next)) = take_delimited(&chars, i, '~', 2) {
                    let mut inner_active = active;
                    inner_active.strike = true;
                    out.push('~');
                    out.push_str(&render_inline_ctx(&inner, inner_active));
                    out.push('~');
                    i = next;
                    continue;
                }
                out.push_str("\\~");
                i += 1;
            }
            // Spoiler `||...||` → MarkdownV2 spoiler `||...||`.
            '|' if is_double(&chars, i, '|') && !active.spoiler => {
                if let Some((inner, next)) = take_delimited(&chars, i, '|', 2) {
                    let mut inner_active = active;
                    inner_active.spoiler = true;
                    out.push_str("||");
                    out.push_str(&render_inline_ctx(&inner, inner_active));
                    out.push_str("||");
                    i = next;
                    continue;
                }
                out.push_str("\\|");
                i += 1;
            }
            // Italic `*...*` or `_..._` → `_..._`. Intraword `_` (flanked by
            // alphanumerics, e.g. `snake_case`) is literal per GFM.
            '*' | '_' if !(active.italic || (c == '_' && is_intraword(&chars, i))) => {
                if let Some((inner, next)) = take_delimited(&chars, i, c, 1) {
                    let mut inner_active = active;
                    inner_active.italic = true;
                    out.push('_');
                    out.push_str(&render_inline_ctx(&inner, inner_active));
                    out.push('_');
                    i = next;
                    continue;
                }
                out.push('\\');
                out.push(c);
                i += 1;
            }
            _ => {
                if MDV2_SPECIAL.contains(&c) {
                    out.push('\\');
                }
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// Is the delimiter at `i` flanked on both sides by alphanumeric characters
/// (i.e. an intraword `_` like in `snake_case`, which GFM treats as literal)?
fn is_intraword(chars: &[char], i: usize) -> bool {
    let prev = i.checked_sub(1).and_then(|p| chars.get(p));
    let next = chars.get(i + 1);
    matches!(prev, Some(c) if c.is_alphanumeric()) && matches!(next, Some(c) if c.is_alphanumeric())
}

/// Is there a doubled delimiter (`cc`) starting at `i`?
fn is_double(chars: &[char], i: usize, c: char) -> bool {
    chars.get(i) == Some(&c) && chars.get(i + 1) == Some(&c)
}

/// Take an inline code span starting at the opening backtick at `i`. Returns
/// the code contents and the index just past the closing backtick.
fn take_inline_code(chars: &[char], i: usize) -> Option<(String, usize)> {
    let mut j = i + 1;
    let mut code = String::new();
    while j < chars.len() {
        if chars[j] == '`' {
            return Some((code, j + 1));
        }
        code.push(chars[j]);
        j += 1;
    }
    None
}

/// Take a delimited inline span (`delim` repeated `count` times on both ends)
/// starting at `i`. Returns the inner text and the index past the closing
/// delimiter. Rejects empty spans so `**` alone isn't treated as a span.
fn take_delimited(chars: &[char], i: usize, delim: char, count: usize) -> Option<(String, usize)> {
    let start = i + count;
    let mut j = start;
    while j < chars.len() {
        if chars[j] == delim {
            let run = run_len(chars, j, delim);
            if count == 1 {
                // A single-delimiter span closes only on a lone delimiter; a
                // doubled (or longer) run belongs to a nested bold/strike span
                // and is skipped over as a unit.
                if run == 1 {
                    if j == start {
                        return None; // empty span
                    }
                    let inner: String = chars[start..j].iter().collect();
                    return Some((inner, j + 1));
                }
                j += run;
                continue;
            }
            if run >= count {
                if j == start {
                    return None; // empty span
                }
                let inner: String = chars[start..j].iter().collect();
                return Some((inner, j + count));
            }
        }
        j += 1;
    }
    None
}

/// Length of the run of `delim` starting at `j`.
fn run_len(chars: &[char], j: usize, delim: char) -> usize {
    let mut n = 0;
    while chars.get(j + n) == Some(&delim) {
        n += 1;
    }
    n
}

/// Take a `[label](url)` link starting at the `[` at `i`. Returns the raw
/// label, raw url, and the index past the closing `)`.
fn take_link(chars: &[char], i: usize) -> Option<(String, String, usize)> {
    if chars.get(i) != Some(&'[') {
        return None;
    }
    let mut j = i + 1;
    let mut label = String::new();
    while j < chars.len() && chars[j] != ']' {
        label.push(chars[j]);
        j += 1;
    }
    if chars.get(j) != Some(&']') || chars.get(j + 1) != Some(&'(') {
        return None;
    }
    j += 2;
    // Balance parentheses inside the URL so targets like `p(1)` aren't
    // truncated at their first inner `)`.
    let mut url = String::new();
    let mut depth = 0usize;
    while j < chars.len() {
        match chars[j] {
            '(' => depth += 1,
            ')' if depth == 0 => break,
            ')' => depth -= 1,
            _ => {}
        }
        url.push(chars[j]);
        j += 1;
    }
    if chars.get(j) != Some(&')') {
        return None;
    }
    Some((label, url, j + 1))
}

/// Render a `[label](url)` link into MarkdownV2, escaping display text and URL
/// in their respective contexts.
fn render_link(label: &str, url: &str, active: Active) -> String {
    format!(
        "[{}]({})",
        render_inline_ctx(label, active),
        escape_link_url(url)
    )
}

// ---- Tables ---------------------------------------------------------------

/// Does a line look like a GFM table row (contains a pipe, non-empty)?
fn is_table_row(line: &str) -> bool {
    let t = line.trim();
    t.contains('|') && !t.is_empty()
}

/// Is the line a GFM table delimiter row (`|---|:--:|`)?
fn is_table_delimiter(line: &str) -> bool {
    let t = line.trim();
    if !t.contains('|') && !t.contains('-') {
        return false;
    }
    let mut saw_dash = false;
    for c in t.chars() {
        match c {
            '-' => saw_dash = true,
            '|' | ':' | ' ' => {}
            _ => return false,
        }
    }
    saw_dash
}

/// Split a table row into trimmed cell strings, dropping the empty edges from
/// leading/trailing pipes.
fn table_cells(row: &str) -> Vec<String> {
    let t = row.trim();
    let t = t.strip_prefix('|').unwrap_or(t);
    let t = t.strip_suffix('|').unwrap_or(t);
    t.split('|').map(|c| c.trim().to_string()).collect()
}

/// Render a GFM pipe table. Small tables become readable per-row bullet groups
/// (`*Header:* value`); large tables fall back to a fenced code block so they
/// never become a garbled wall of pipes.
fn render_table(header: &str, _delimiter: &str, rows: &[&str]) -> String {
    let headers = table_cells(header);
    if rows.len() > TABLE_ROW_BULLET_LIMIT {
        return render_table_as_code(header, rows);
    }

    let mut groups: Vec<String> = Vec::new();
    for row in rows {
        let cells = table_cells(row);
        let mut lines: Vec<String> = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            let key = headers.get(idx).map(String::as_str).unwrap_or("");
            if key.is_empty() {
                lines.push(format!("\\- {}", render_inline(cell)));
            } else {
                lines.push(format!(
                    "\\- *{}:* {}",
                    render_inline(key),
                    render_inline(cell)
                ));
            }
        }
        groups.push(lines.join("\n"));
    }
    groups.join("\n\n")
}

/// Fallback: render a table as a plain monospace grid inside a code block.
fn render_table_as_code(header: &str, rows: &[&str]) -> String {
    let mut all: Vec<Vec<String>> = Vec::new();
    all.push(table_cells(header));
    for row in rows {
        all.push(table_cells(row));
    }
    let cols = all.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0usize; cols];
    for r in &all {
        for (width, cell) in widths.iter_mut().zip(r.iter()) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut lines: Vec<String> = Vec::new();
    for r in &all {
        let mut cells: Vec<String> = Vec::new();
        for (c, width) in widths.iter().enumerate().take(cols) {
            let cell = r.get(c).map(String::as_str).unwrap_or("");
            cells.push(format!("{cell:width$}"));
        }
        lines.push(cells.join("  ").trim_end().to_string());
    }
    render_code_block("", &lines.iter().map(String::as_str).collect::<Vec<_>>())
}

// ---- Chunking -------------------------------------------------------------

/// Reserve room for a worst-case `(N/M)` indicator plus its separator so the
/// indicator never pushes a chunk past the limit.
const INDICATOR_RESERVE: usize = 16;

/// Source-chunk budget. Chunking happens on the *raw source* first, then each
/// source chunk is rendered independently. MarkdownV2 escaping can at most
/// roughly double a source chunk's length (a backslash before each char), so
/// keeping the source chunk under half the limit guarantees the rendered chunk
/// (plus indicator) stays under Telegram's 4096-UTF-16 cap. Chunking the
/// *source* also keeps rich and plain fallbacks byte-for-byte aligned per
/// chunk, which is what makes “never lose the message” hold: a parse error on a
/// rich chunk drops to the plain rendering of the *same* source span.
const SOURCE_BUDGET: usize = (TELEGRAM_MAX_UTF16 - INDICATOR_RESERVE) / 2;

/// Split the raw markdown into source chunks on line boundaries. Fenced code
/// blocks are never cut (an oversized fence is re-wrapped with its own closing/
/// opening fence on each boundary), and GFM tables are kept atomic so a header
/// row is never separated from its data rows. `render_chunks` applies a final
/// post-render safety net for the rare block that still expands past the cap.
fn split_source_into_chunks(markdown: &str) -> Vec<String> {
    let lines: Vec<&str> = markdown.split('\n').collect();
    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut fence: Option<&'static str> = None;
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];

        // A whole GFM table (header + delimiter + data rows) is treated as one
        // atomic unit: flush the current buffer, then emit the table as its own
        // chunk(s). Keeping it whole means `to_markdown_v2` always sees a
        // complete table (never an orphan header) and can route an oversized
        // one to a fenced code block, which the re-wrap path then chunks safely.
        if fence.is_none()
            && is_table_row(line)
            && i + 1 < lines.len()
            && is_table_delimiter(lines[i + 1])
        {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            let header = lines[i];
            let delimiter = lines[i + 1];
            i += 2; // header + delimiter
            let rows_start = i;
            while i < lines.len() && is_table_row(lines[i]) {
                i += 1;
            }
            for piece in split_table_source(header, delimiter, &lines[rows_start..i]) {
                chunks.push(piece);
            }
            continue;
        }

        let marker = fence_marker(line.trim_start());
        let opening_fence = fence.is_none() && marker.is_some();
        match (fence, marker) {
            (None, Some(m)) => fence = Some(m),
            (Some(f), Some(m)) if m == f => fence = None,
            _ => {}
        }

        let line_len = utf16_len(line);
        let addition = if current.is_empty() {
            line_len
        } else {
            line_len + 1
        };

        // Flush before adding if we'd overflow and we are not mid-fence (a
        // fence body is handled by the re-wrap path below).
        if !current.is_empty()
            && utf16_len(&current) + addition > SOURCE_BUDGET
            && fence.is_none()
            && !opening_fence
        {
            chunks.push(std::mem::take(&mut current));
        }

        // Inside a fence, if adding this line would overflow, close the fence,
        // flush, and re-open it so the block continues legibly in a new chunk.
        if let Some(f) = fence {
            if !current.is_empty() && utf16_len(&current) + addition > SOURCE_BUDGET {
                push_source_line(&mut current, f);
                chunks.push(std::mem::take(&mut current));
                current.push_str(f);
            }
        }

        push_source_line(&mut current, line);

        // A single source line longer than the budget is hard-split by UTF-16.
        while utf16_len(&current) > SOURCE_BUDGET {
            let (head, tail) = split_at_utf16(&current, SOURCE_BUDGET);
            chunks.push(head);
            current = tail;
        }
        i += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() {
        chunks.push(String::new());
    }
    chunks
}

fn push_source_line(current: &mut String, line: &str) {
    if !current.is_empty() {
        current.push('\n');
    }
    current.push_str(line);
}

/// Split a GFM table's source into pieces that each fit the source budget,
/// repeating the header + delimiter on every piece so no piece is an orphan
/// header and each renders as a complete table. A single row wider than the
/// budget is emitted on its own (the post-render safety net catches its
/// expansion). The header+delimiter+rows always stay together per piece, so the
/// header is never silently dropped.
fn split_table_source(header: &str, delimiter: &str, rows: &[&str]) -> Vec<String> {
    let prefix = format!("{header}\n{delimiter}");
    if rows.is_empty() {
        return vec![prefix];
    }
    let mut pieces: Vec<String> = Vec::new();
    let mut current = prefix.clone();
    for row in rows {
        let addition = utf16_len(row) + 1;
        if utf16_len(&current) > utf16_len(&prefix)
            && utf16_len(&current) + addition > SOURCE_BUDGET
        {
            pieces.push(std::mem::take(&mut current));
            current = prefix.clone();
        }
        push_source_line(&mut current, row);
    }
    if utf16_len(&current) > utf16_len(&prefix) {
        pieces.push(current);
    }
    if pieces.is_empty() {
        pieces.push(prefix);
    }
    pieces
}

/// Split `s` at (at most) `limit` UTF-16 code units, on a `char` boundary.
fn split_at_utf16(s: &str, limit: usize) -> (String, String) {
    let mut head = String::new();
    let mut used = 0;
    let mut chars = s.chars();
    for ch in chars.by_ref() {
        let w = ch.len_utf16();
        if used + w > limit {
            let mut tail = String::new();
            tail.push(ch);
            tail.extend(chars);
            return (head, tail);
        }
        head.push(ch);
        used += w;
    }
    (head, String::new())
}

/// Append an escaped `(N/M)` indicator to a rendered MarkdownV2 chunk, kept on
/// its own line so it can't fuse with a trailing code fence and break parsing.
fn with_indicator_mdv2(rendered: &str, idx: usize, total: usize) -> String {
    if total <= 1 {
        return rendered.to_string();
    }
    let indicator = escape_mdv2(&format!("({}/{})", idx + 1, total));
    format!("{rendered}\n\n{indicator}")
}

/// Convert an agent's markdown reply into a sequence of ready-to-send chunks,
/// each already carrying the `parse_mode` it must be sent with and the raw
/// source span it covers. This is the primary rich tier of the fallback chain;
/// the caller drops a rejected chunk to a plain send of its `source`.
pub fn render_chunks(markdown: &str) -> Vec<Chunk> {
    // Pass 1: turn each source span into a body + parse_mode, guaranteeing each
    // body fits the cap even before the `(N/M)` indicator is appended.
    let mut bodies: Vec<(String, ParseMode, String)> = Vec::new();
    let budget = TELEGRAM_MAX_UTF16 - INDICATOR_RESERVE;
    for source in split_source_into_chunks(markdown) {
        let rendered = to_markdown_v2(&source);
        if utf16_len(&rendered) <= budget {
            bodies.push((rendered, ParseMode::MarkdownV2, source));
            continue;
        }
        // Safety net: some blocks (notably wide tables whose header repeats
        // into every row) expand past the cap. Drop this span to plain text
        // (always shorter than its MarkdownV2 rendering); if the plain form is
        // itself over the cap, hard-split it by UTF-16 so every piece is
        // deliverable. This keeps “never lose the message” for pathological
        // inputs.
        let mut rest = source;
        while utf16_len(&rest) > budget {
            let (head, tail) = split_at_utf16(&rest, budget);
            bodies.push((head.clone(), ParseMode::Plain, head));
            rest = tail;
        }
        if !rest.is_empty() {
            bodies.push((rest.clone(), ParseMode::Plain, rest));
        }
    }

    // Pass 2: append the `(N/M)` indicator over the final chunk count, escaping
    // it for MarkdownV2 chunks and leaving it literal for plain ones.
    let total = bodies.len();
    bodies
        .into_iter()
        .enumerate()
        .map(|(idx, (body, mode, source))| {
            let text = match mode {
                ParseMode::MarkdownV2 => with_indicator_mdv2(&body, idx, total),
                ParseMode::Plain => with_indicator_plain(&body, idx, total),
            };
            Chunk {
                text,
                parse_mode: mode,
                source,
            }
        })
        .collect()
}

/// Append an unescaped `(N/M)` indicator for a plain-text chunk.
fn with_indicator_plain(body: &str, idx: usize, total: usize) -> String {
    if total <= 1 {
        body.to_string()
    } else {
        format!("{body}\n\n({}/{})", idx + 1, total)
    }
}

/// The guaranteed floor: emit the *original* markdown as plain-text chunks with
/// no `parse_mode`, so a reply that can't be safely formatted still arrives.
/// The live send path builds its per-chunk plain fallback inline from a rich
/// chunk's `source`; this whole-reply form is kept as module API for callers
/// that want to go straight to plain text.
#[cfg_attr(not(test), allow(dead_code))]
pub fn plain_chunks(markdown: &str) -> Vec<Chunk> {
    let sources = split_source_into_chunks(markdown);
    let total = sources.len();
    sources
        .into_iter()
        .enumerate()
        .map(|(idx, source)| Chunk {
            text: with_indicator_plain(&source, idx, total),
            parse_mode: ParseMode::Plain,
            source,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md(s: &str) -> String {
        to_markdown_v2(s)
    }

    #[test]
    fn bold_maps_to_single_star() {
        assert_eq!(md("**bold**"), "*bold*");
        assert_eq!(md("__bold__"), "*bold*");
    }

    #[test]
    fn italic_maps_to_underscore() {
        assert_eq!(md("*italic*"), "_italic_");
        assert_eq!(md("_italic_"), "_italic_");
    }

    #[test]
    fn strikethrough_maps_to_single_tilde() {
        assert_eq!(md("~~gone~~"), "~gone~");
    }

    #[test]
    fn inline_code_is_protected() {
        // The `.` inside code is NOT escaped; outside it would be.
        assert_eq!(md("call `a.b()` now"), "call `a.b()` now");
        // A period in plain text IS escaped.
        assert_eq!(md("end."), "end\\.");
    }

    #[test]
    fn inline_code_escapes_backtick_and_backslash() {
        assert_eq!(md("`a\\b`"), "`a\\\\b`");
    }

    #[test]
    fn plain_specials_are_escaped() {
        assert_eq!(md("a-b.c!"), "a\\-b\\.c\\!");
        assert_eq!(md("1 + 1 = 2"), "1 \\+ 1 \\= 2");
    }

    #[test]
    fn heading_becomes_bold() {
        assert_eq!(md("## Title"), "*Title*");
        assert_eq!(md("# H1"), "*H1*");
        assert_eq!(md("###### H6"), "*H6*");
    }

    #[test]
    fn seven_hashes_is_not_a_heading() {
        assert_eq!(md("####### too many"), "\\#\\#\\#\\#\\#\\#\\# too many");
    }

    #[test]
    fn link_is_converted_and_escaped() {
        assert_eq!(
            md("[the docs](https://x.io/a_b)"),
            "[the docs](https://x.io/a_b)"
        );
        // Display text specials are escaped; url inner `)` is escaped.
        assert_eq!(md("[a.b](http://h/p(1))"), "[a\\.b](http://h/p(1\\))");
        assert_eq!(md("[a.b](http://h/p)"), "[a\\.b](http://h/p)");
    }

    #[test]
    fn image_renders_as_link() {
        assert_eq!(md("![alt](http://h/i.png)"), "[alt](http://h/i.png)");
    }

    #[test]
    fn fenced_code_block_preserves_body_and_lang() {
        let input = "```rust\nlet x = 1.0;\n```";
        assert_eq!(md(input), "```rust\nlet x = 1.0;\n```");
    }

    #[test]
    fn fenced_code_block_no_lang() {
        let input = "```\nplain.text!\n```";
        assert_eq!(md(input), "```\nplain.text!\n```");
    }

    #[test]
    fn blockquote_preserves_marker() {
        assert_eq!(md("> quoted."), ">quoted\\.");
    }

    #[test]
    fn bullet_list_renders_dash() {
        assert_eq!(md("- item one"), "\\- item one");
        assert_eq!(md("* item two"), "\\- item two");
    }

    #[test]
    fn task_list_renders_checkbox() {
        assert_eq!(md("- [ ] todo"), "☐ todo");
        assert_eq!(md("- [x] done"), "☑ done");
        assert_eq!(md("- [X] done"), "☑ done");
    }

    #[test]
    fn spoiler_is_preserved() {
        assert_eq!(md("||secret||"), "||secret||");
    }

    #[test]
    fn nested_bold_inside_italic() {
        assert_eq!(md("*a **b** c*"), "_a *b* c_");
    }

    #[test]
    fn unclosed_markers_are_escaped() {
        assert_eq!(md("a ** b"), "a \\*\\* b");
        assert_eq!(md("stray _ char"), "stray \\_ char");
    }

    #[test]
    fn small_table_flattens_to_bullets() {
        let input = "| Name | Age |\n|------|-----|\n| Ada | 36 |\n| Bo | 40 |";
        let out = md(input);
        assert_eq!(
            out,
            "\\- *Name:* Ada\n\\- *Age:* 36\n\n\\- *Name:* Bo\n\\- *Age:* 40"
        );
    }

    #[test]
    fn large_table_falls_back_to_code_block() {
        let mut rows = vec![String::from("| N |"), String::from("|---|")];
        for r in 0..9 {
            rows.push(format!("| {r} |"));
        }
        let out = md(&rows.join("\n"));
        assert!(out.starts_with("```\n"));
        assert!(out.ends_with("\n```"));
        assert!(out.contains("N"));
    }

    #[test]
    fn utf16_len_counts_code_units() {
        assert_eq!(utf16_len("abc"), 3);
        assert_eq!(utf16_len("😀"), 2); // astral char = 2 UTF-16 units
    }

    #[test]
    fn single_chunk_has_no_indicator() {
        let chunks = render_chunks("short body");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "short body");
    }

    #[test]
    fn long_body_is_chunked_with_escaped_indicator() {
        let line = "x".repeat(1000);
        let body = vec![line; 10].join("\n");
        let chunks = render_chunks(&body);
        assert!(chunks.len() >= 2);
        // Indicators are escaped MarkdownV2: `\(1/N\)`.
        assert!(chunks[0].text.contains("\\("));
        assert!(chunks[0].text.contains("/"));
        assert!(chunks[0].text.contains("\\)"));
        for c in &chunks {
            assert!(utf16_len(&c.text) <= TELEGRAM_MAX_UTF16);
        }
    }

    #[test]
    fn small_fence_stays_in_one_chunk() {
        let code = "a\n".repeat(50);
        let body = format!("```\n{code}```");
        let chunks = split_source_into_chunks(&body);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].matches("```").count(), 2);
    }

    #[test]
    fn large_fence_is_rewrapped_across_chunks() {
        // A code block larger than the source budget must be re-wrapped: every
        // chunk is a self-contained fenced block (balanced ``` pairs), so each
        // renders as valid MarkdownV2 on its own.
        let code = "line\n".repeat(2000);
        let body = format!("```\n{code}```");
        let chunks = split_source_into_chunks(&body);
        assert!(chunks.len() >= 2, "expected the big fence to be split");
        for c in &chunks {
            assert_eq!(
                c.matches("```").count() % 2,
                0,
                "each chunk must have balanced fences: {c:?}"
            );
        }
        // Rendered chunks stay under the Telegram cap.
        for c in render_chunks(&body) {
            assert!(utf16_len(&c.text) <= TELEGRAM_MAX_UTF16);
        }
    }

    #[test]
    fn hard_split_of_a_single_long_line() {
        let line = "y".repeat(TELEGRAM_MAX_UTF16 * 2);
        let chunks = render_chunks(&line);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(utf16_len(&c.text) <= TELEGRAM_MAX_UTF16);
        }
    }

    #[test]
    fn chunk_source_covers_the_whole_reply_losslessly() {
        // Every non-structural character of the original reply appears across
        // the chunk `source` spans — the invariant that makes the plain-text
        // fallback lossless. (Chunking adds `\n` line breaks and re-wrapped
        // ``` fences, so compare with newlines and fence lines removed.)
        let line = "abcXYZ0189 ".repeat(500);
        let body = format!("# Heading\n\n{line}\n\n```\n{}\n```", "code\n".repeat(400));
        let chunks = render_chunks(&body);
        let normalize = |s: &str| {
            s.lines()
                .filter(|l| l.trim() != "```")
                .collect::<String>()
                .replace(' ', "")
        };
        let joined: String = chunks.iter().map(|c| normalize(&c.source)).collect();
        assert_eq!(joined, normalize(&body));
    }

    #[test]
    fn plain_chunks_never_add_parse_mode() {
        let chunks = plain_chunks("**not converted** stays raw.");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].parse_mode, ParseMode::Plain);
        assert_eq!(chunks[0].text, "**not converted** stays raw.");
    }

    #[test]
    fn plain_chunks_add_unescaped_indicator_when_split() {
        let line = "z".repeat(1600);
        let body = format!("{line}\n{line}");
        let chunks = plain_chunks(&body);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].text.ends_with("(1/2)"));
        assert!(chunks[1].text.ends_with("(2/2)"));
        for c in &chunks {
            assert_eq!(c.parse_mode, ParseMode::Plain);
        }
    }

    #[test]
    fn render_chunks_tags_markdown_v2() {
        let chunks = render_chunks("**hi**");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].parse_mode, ParseMode::MarkdownV2);
        assert_eq!(chunks[0].text, "*hi*");
        assert_eq!(chunks[0].source, "**hi**");
    }

    #[test]
    fn adjacent_bold_spans_do_not_illegally_nest() {
        // Greedy first-close means `**a **b** c**` is two separate bold spans
        // with a literal `b` between — valid MarkdownV2, not an illegal
        // same-type nesting.
        assert_eq!(md("**a **b** c**"), "*a *b* c*");
    }

    #[test]
    fn same_type_nesting_emits_inner_delimiters_literally() {
        // A genuine nested emphasis of the same type (bold inside bold) would
        // be rejected by Telegram; the inner `*italic*`-style single markers
        // stay literal when already inside that entity. Here italic-in-italic:
        // `_a _b_ c_` closes at the first inner `_`, leaving ` c_` literal.
        assert_eq!(md("*outer **still bold** end*"), "_outer *still bold* end_");
    }

    #[test]
    fn intraword_underscore_is_literal() {
        assert_eq!(md("snake_case_here"), "snake\\_case\\_here");
        // A real italic span at word boundaries still converts.
        assert_eq!(md("_hi_ there"), "_hi_ there");
    }

    #[test]
    fn every_rendered_chunk_stays_under_the_cap() {
        // A wide table whose header repeats into every row expands far beyond
        // 2x; the post-render safety net must keep every chunk deliverable.
        let header = format!("| {} | {} |", "H1".repeat(120), "H2".repeat(120));
        let mut rows = vec![header, "|---|---|".to_string()];
        for r in 0..6 {
            rows.push(format!("| a{r} | b{r} |"));
        }
        let chunks = render_chunks(&rows.join("\n"));
        assert!(!chunks.is_empty());
        for c in &chunks {
            assert!(
                utf16_len(&c.text) <= TELEGRAM_MAX_UTF16,
                "chunk over cap: {} units",
                utf16_len(&c.text)
            );
        }
    }

    #[test]
    fn oversized_table_falls_back_to_plain_not_dropped() {
        // A table that renders past the cap drops to plain text (never a
        // rejected over-long send, never silent loss).
        let cell = "X".repeat(2500);
        let table = format!("| A | B |\n|---|---|\n| {cell} | {cell} |");
        let chunks = render_chunks(&table);
        assert!(chunks.iter().any(|c| c.parse_mode == ParseMode::Plain));
        // The content survives and every chunk is deliverable (≤ cap).
        let joined: String = chunks.iter().map(|c| c.source.clone()).collect();
        assert!(joined.contains(&cell));
        for c in &chunks {
            assert!(
                utf16_len(&c.text) <= TELEGRAM_MAX_UTF16,
                "chunk over cap: {}",
                utf16_len(&c.text)
            );
        }
    }

    #[test]
    fn split_table_keeps_header_with_rows() {
        // A table forced across chunks must never leave a header orphaned in a
        // chunk with no data rows (which would render to an empty body and
        // silently drop the header). The table is kept atomic.
        let mut rows = vec!["| Name | Val |".to_string(), "|------|-----|".to_string()];
        for r in 0..7 {
            rows.push(format!("| name{r} | {} |", "v".repeat(300)));
        }
        let doc = format!("intro line\n\n{}\n\noutro line", rows.join("\n"));
        let chunks = render_chunks(&doc);
        // No chunk is just an indicator with no real content.
        for c in &chunks {
            let stripped = c.text.replace(['\\', '(', ')', '/', '\n'], "");
            let only_digits =
                !stripped.is_empty() && stripped.chars().all(|ch| ch.is_ascii_digit());
            assert!(
                !only_digits,
                "chunk collapsed to a bare indicator: {:?}",
                c.text
            );
        }
        // Header content survives somewhere.
        assert!(chunks.iter().any(|c| c.source.contains("Name")));
    }

    #[test]
    fn parse_mode_api_value() {
        assert_eq!(ParseMode::MarkdownV2.as_api_value(), Some("MarkdownV2"));
        assert_eq!(ParseMode::Plain.as_api_value(), None);
    }

    #[test]
    fn multiline_document_round_trip() {
        let input =
            "# Report\n\nStatus: **ok**\n\nSee `main.rs` and [repo](http://h/r).\n\n> note: done.";
        let out = md(input);
        let expected =
            "*Report*\n\nStatus: *ok*\n\nSee `main.rs` and [repo](http://h/r)\\.\n\n>note: done\\.";
        assert_eq!(out, expected);
    }
}
