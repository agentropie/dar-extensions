//! Light masking of obvious secrets in activity labels. Not exhaustive.

use std::sync::LazyLock;

use regex::Regex;

pub const MASK: &str = "•••";

const KEY: &str = r"[A-Za-z0-9_-]*(?i:token|secret|password|passwd|api[_-]?key)[A-Za-z0-9_-]*";

static RULES: LazyLock<Vec<(Regex, String)>> = LazyLock::new(|| {
    let r = |p: &str, to: &str| (Regex::new(p).unwrap(), to.to_string());
    vec![
        r(
            r#"(?i)(?-u:\b)(authorization\s*:\s*)(bearer\s+|basic\s+)?[^\s"'`]+"#,
            &format!("${{1}}${{2}}{MASK}"),
        ),
        r(
            r"(?i)(?-u:\b)(bearer)\s+[A-Za-z0-9._~+/=-]+",
            &format!("$1 {MASK}"),
        ),
        r(
            &format!(r#"("{KEY}"\s*:\s*)"(?:[^"\\]|\\.)*""#),
            &format!("$1\"{MASK}\""),
        ),
        r(
            &format!(r"((?:--)?{KEY}(?:=|\s+))[^\s&;|]+"),
            &format!("$1{MASK}"),
        ),
        r(r"(://)[^\s/@]+@", &format!("$1{MASK}@")),
        r(
            r"(?-u:\b)(?:ghp_|ghs_|github_pat_|sk-ant-|sk-|xox[a-z]-|lin_api_|AKIA)[A-Za-z0-9_-]{6,}",
            MASK,
        ),
        r(r"(?-u:\b)eyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", MASK),
    ]
});

pub fn redact(text: &str) -> String {
    RULES.iter().fold(text.to_string(), |out, (re, to)| {
        re.replace_all(&out, to.as_str()).into_owned()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_obvious_secrets() {
        for (input, secret) in [
            ("curl -H 'Authorization: Bearer abc123'", "abc123"),
            ("GITHUB_TOKEN=gh123 npm publish", "gh123"),
            ("--api-key s3cr3t", "s3cr3t"),
            (r#"{"client_secret": "shh"}"#, "shh"),
            ("git clone https://user:pass@github.com/x", "user:pass"),
            ("use ghp_abcdefghij1234", "ghp_abc"),
            ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig", "eyJ"),
        ] {
            let out = redact(input);
            assert!(!out.contains(secret), "{input} -> {out}");
            assert!(out.contains(MASK), "{input} -> {out}");
        }
    }

    #[test]
    fn keeps_ordinary_text() {
        for text in ["git status", "cargo test -p threadit", "rg -n foo src/"] {
            assert_eq!(redact(text), text);
        }
    }
}
