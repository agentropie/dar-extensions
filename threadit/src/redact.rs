//! Mask secrets in activity text. A second layer: bash labels are first cut
//! by `bash_label`. Conservative, and idempotent: masked values are left alone.
//! Port of the Yoplai Threadit `redact.ts`.

use std::sync::LazyLock;

use regex::{Captures, Regex};

pub const MASK: &str = "•••";

const SENSITIVE_SEGMENTS: &[&str] = &[
    "key",
    "keys",
    "auth",
    "authorization",
    "session",
    "sid",
    "sig",
    "signature",
    "pin",
    "otp",
    "bearer",
    "jwt",
    "pat",
];

static SENSITIVE_LETTERS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("token|secret|passw|credential|cookie|apikey|privatekey").unwrap());
static CAMEL: LazyLock<Regex> = LazyLock::new(|| Regex::new("([a-z0-9])([A-Z])").unwrap());

/// True when a key, flag or variable name suggests a credential.
pub fn sensitive_name(name: &str) -> bool {
    let letters: String = name
        .to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_lowercase())
        .collect();
    if SENSITIVE_LETTERS.is_match(&letters) {
        return true;
    }
    CAMEL
        .replace_all(name, "${1}_${2}")
        .to_lowercase()
        .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .any(|s| SENSITIVE_SEGMENTS.contains(&s) || s.ends_with("pass") || s.ends_with("pwd"))
}

/// One shell-ish value: quoted parts, escapes and plain chars, concatenated.
const VALUE: &str = r#"(?:"(?:[^"\\]|\\.)*"|'[^']*'|\\.|[^\s&,;"'])+"#;

fn re(pattern: &str) -> Regex {
    Regex::new(pattern).unwrap()
}

/// Replace the trailing `value` of `all` with the mask.
fn mask_tail(all: &str, value: &str) -> String {
    format!("{}{MASK}", &all[..all.len() - value.len()])
}

/// Mask capture `value_at` when capture `name_at` is a sensitive name.
fn mask_if(caps: &Captures, name_at: usize, value_at: usize) -> String {
    let all = &caps[0];
    let value = caps.get(value_at).map_or("", |m| m.as_str());
    let name = caps.get(name_at).map_or("", |m| m.as_str());
    if !sensitive_name(name) || value.contains(MASK) {
        all.to_string()
    } else {
        mask_tail(all, value)
    }
}

type Replacer = fn(&Captures) -> String;

static RULES: LazyLock<Vec<(Regex, Replacer)>> = LazyLock::new(|| {
    vec![
        // URL credentials and query strings
        (re(r#"(?i)([a-z][a-z0-9+.-]*://)[^\s/@"'`]+@"#), |c| {
            format!("{}{MASK}@", &c[1])
        }),
        (
            re(r#"(?i)(\b[a-z][a-z0-9+.-]*://[^\s?#"'`]*)\?[^\s"'`#]+"#),
            |c| format!("{}?{MASK}", &c[1]),
        ),
        // Credential headers: everything to the end of the header value
        (
            re(
                r#"(?i)\b((?:proxy-)?authorization|(?:set-)?cookie|x-api-key|x-auth-token)(\s*:\s*)[^\n"'`]+"#,
            ),
            |c| {
                let value = &c[0][c[1].len() + c[2].len()..];
                if value.trim_start().starts_with(MASK) {
                    c[0].to_string()
                } else {
                    format!("{}{}{MASK}", &c[1], &c[2])
                }
            },
        ),
        (re(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{4,}"), |c| {
            format!("{} {MASK}", &c[1])
        }),
        // "password": "x", \"token\":\"x\", 'pin': 1234
        (
            re(
                r#"(?:"([A-Za-z0-9_.-]+)"|\\"([A-Za-z0-9_.-]+)\\"|'([A-Za-z0-9_.-]+)')(\s*:\s*)(\\?"(?:[^"\\]|\\[^"])*\\?"|[^,}\s]+)"#,
            ),
            |c| {
                let all = &c[0];
                let name = c
                    .get(1)
                    .or(c.get(2))
                    .or(c.get(3))
                    .map_or("", |m| m.as_str());
                let value = &c[5];
                if !sensitive_name(name) || value.contains(MASK) {
                    return all.to_string();
                }
                let quote = if value.starts_with("\\\"") {
                    "\\\""
                } else if value.starts_with('"') {
                    "\""
                } else {
                    ""
                };
                format!("{}{quote}{MASK}{quote}", &all[..all.len() - value.len()])
            },
        ),
        // Values after credential-carrying curl/ssh-style flags (-u user:pass, -H ...)
        (
            re(&format!(
                r"((?:^|\s)(?:-[uHdpbe]|--(?:user|header|data[A-Za-z0-9_-]*|cookie|proxy-user|form)))(\s+|=)({VALUE})"
            )),
            |c| {
                let value = &c[3];
                // "…" is the cut marker of a bash label, not a value.
                if value.contains(MASK) || value == "…" {
                    c[0].to_string()
                } else {
                    mask_tail(&c[0], value)
                }
            },
        ),
        // --api-key value / --token=value
        (
            re(&format!(r"(--?)([A-Za-z][A-Za-z0-9_-]*)(\s+|=)({VALUE})")),
            |c| mask_if(c, 2, 4),
        ),
        // API_KEY=value / password: value
        (
            re(&format!(
                r"\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*[=:]\s*)({VALUE})"
            )),
            |c| mask_if(c, 1, 3),
        ),
        // Well-known token formats
        (
            re(r"\b(?:gh[psoru]_|github_pat_|sk-ant-|sk-|xox[abprs]-|lin_api_)[A-Za-z0-9_-]{6,}"),
            |_| MASK.to_string(),
        ),
        (re(r"\b(?:AKIA|ASIA)[0-9A-Z]{12,}"), |_| MASK.to_string()),
        (re(r"\beyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*"), |_| {
            MASK.to_string()
        }),
        // Long runs of base64/hex chars with a digit (a length rule, not real entropy)
        (re(r"[A-Za-z0-9+_=-]*[0-9][A-Za-z0-9+_=-]*"), |c| {
            if c[0].len() >= 32 {
                MASK.to_string()
            } else {
                c[0].to_string()
            }
        }),
    ]
});

pub fn redact(text: &str) -> String {
    let mut out = text.to_string();
    for (pattern, replacer) in RULES.iter() {
        out = pattern.replace_all(&out, *replacer).into_owned();
    }
    out
}

/// True when a single word looks like a token by itself.
pub fn looks_like_token(word: &str) -> bool {
    redact(word) != word
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_secrets() {
        for (input, secret) in [
            ("Authorization: Bearer abcdef123", "abcdef123"),
            ("curl -H 'authorization: token xyz789'", "xyz789"),
            ("bearer sometokenvalue", "sometokenvalue"),
            ("GITHUB_TOKEN=gh123 npm publish", "gh123"),
            ("export DB_PASSWORD='p a s s'", "p a s s"),
            ("--api-key s3cr3t", "s3cr3t"),
            ("--token=abc", "abc"),
            ("--password hunter2", "hunter2"),
            (r#"{"client_secret": "shh"}"#, "shh"),
            (r#"{"apiKey":"k1"}"#, "k1"),
            ("Cookie: session=abc", "session=abc"),
            ("auth=zzz", "zzz"),
            ("credentials: c1", "c1"),
            ("git clone https://user:pass@github.com/x", "user:pass"),
            ("ghs_abcdefghij1234", "ghs_abcdefghij1234"),
            ("ghp_abcdefghij1234", "ghp_"),
            ("gho_abcdefghij1234", "gho_"),
            ("github_pat_11ABCDEFG_xyz", "github_pat_"),
            ("sk-proj-abcdefghijk", "sk-proj"),
            ("sk-ant-api03-abcdefgh", "sk-ant"),
            ("xoxb-1234-5678-abcd", "xoxb"),
            ("xoxp-1234-5678-abcd", "xoxp"),
            ("lin_api_abcdefghijkl", "lin_api_"),
            ("AKIAIOSFODNN7EXAMPLE", "AKIA"),
            ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig", "eyJ"),
            (
                "see 0123456789abcdef0123456789abcdef01 here",
                "0123456789abcdef",
            ),
            (
                "blob QWxhZGRpbjpvcGVuIHNlc2FtZQ0aGVsbG8gd29ybGQ=",
                "QWxhZGRpbj",
            ),
        ] {
            let out = redact(input);
            assert!(!out.contains(secret), "{input} -> {out}");
            assert!(out.contains(MASK), "{input} -> {out}");
        }
    }

    #[test]
    fn keeps_ordinary_text() {
        for text in [
            "Reading src/labels.ts",
            "git status",
            "Checked the deploy of v1.2.3",
            "pnpm exec vitest run packages/extensions/threadit/src/labels.test.ts",
        ] {
            assert_eq!(redact(text), text);
        }
    }
}
