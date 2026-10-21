//! `extensions.threadit` configuration and the `THREADIT_TOKEN` secret.

use std::time::Duration;

use anyhow::{bail, Result};
use dar_extension_sdk::ConfigStore;
use serde::Deserialize;

pub const TOKEN_ENV: &str = "THREADIT_TOKEN";
const DEFAULT_POLL_MS: u64 = 5000;
const MIN_POLL_MS: u64 = 1000;
const DEFAULT_TURN_TIMEOUT_MINUTES: u64 = 60;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreaditConfig {
    pub base_url: Option<String>,
    pub poll_interval_ms: Option<u64>,
    pub backend: Option<String>,
    pub turn_timeout_minutes: Option<u64>,
    pub activity: ActivityMode,
}

/// How much tool detail activity labels show.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivityMode {
    /// Tool names only.
    Safe,
    /// Tool names plus the path, command or first argument, lightly masked.
    #[default]
    Detailed,
}

impl ThreaditConfig {
    /// The base URL without a trailing slash. Fails when it is missing or not http(s).
    /// A `$env:NAME` value reads the URL from the environment variable `NAME`.
    pub fn base_url(&self) -> Result<String> {
        self.base_url_with(|name| std::env::var(name).ok())
    }

    fn base_url_with(&self, env: impl Fn(&str) -> Option<String>) -> Result<String> {
        let raw = self.base_url.as_deref().map(str::trim).unwrap_or("");
        let resolved;
        let raw = match raw.strip_prefix("$env:") {
            Some(name) => {
                resolved = env(name.trim()).unwrap_or_default();
                if resolved.trim().is_empty() {
                    bail!("threadit.base_url refers to {raw}, but that variable is not set");
                }
                resolved.trim()
            }
            None => raw,
        };
        if raw.is_empty() {
            bail!("threadit.base_url is required: set extensions.threadit.base_url in agent.yaml");
        }
        if !(raw.starts_with("http://") || raw.starts_with("https://")) {
            bail!("threadit.base_url must start with http:// or https://");
        }
        Ok(raw.trim_end_matches('/').to_string())
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(
            self.poll_interval_ms
                .unwrap_or(DEFAULT_POLL_MS)
                .max(MIN_POLL_MS),
        )
    }

    /// Longest time one turn may run. At least one minute.
    pub fn turn_timeout(&self) -> Duration {
        Duration::from_secs(
            60 * self
                .turn_timeout_minutes
                .unwrap_or(DEFAULT_TURN_TIMEOUT_MINUTES)
                .max(1),
        )
    }

    pub fn backend(&self) -> Option<&str> {
        self.backend.as_deref().filter(|b| !b.trim().is_empty())
    }
}

pub fn parse_config(config: &ConfigStore, id: &str) -> Result<ThreaditConfig> {
    match config.get(id) {
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(ThreaditConfig::default()),
    }
}

/// Read the token from the environment. The error names the variable, never a value.
pub fn token() -> Result<String> {
    token_from(std::env::var(TOKEN_ENV).ok())
}

fn token_from(value: Option<String>) -> Result<String> {
    match value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        Some(token) => Ok(token),
        None => bail!("{TOKEN_ENV} is required: set it in the agent .env file"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_is_required_and_trimmed() {
        let mut cfg = ThreaditConfig::default();
        assert!(cfg.base_url().unwrap_err().to_string().contains("base_url"));
        cfg.base_url = Some("ftp://x".into());
        assert!(cfg.base_url().is_err());
        cfg.base_url = Some(" http://127.0.0.1:8000/ ".into());
        assert_eq!(cfg.base_url().unwrap(), "http://127.0.0.1:8000");
    }

    #[test]
    fn base_url_resolves_env_reference() {
        let cfg = ThreaditConfig {
            base_url: Some("$env:THREADIT_BASE_URL".into()),
            ..Default::default()
        };
        let env = |name: &str| {
            (name == "THREADIT_BASE_URL").then(|| " https://board.example/ ".to_string())
        };
        assert_eq!(cfg.base_url_with(env).unwrap(), "https://board.example");
        let err = cfg.base_url_with(|_| None).unwrap_err().to_string();
        assert!(err.contains("$env:THREADIT_BASE_URL"), "{err}");
        assert!(cfg.base_url_with(|_| Some("ftp://x".into())).is_err());
    }

    #[test]
    fn poll_interval_has_default_and_floor() {
        let mut cfg = ThreaditConfig::default();
        assert_eq!(cfg.poll_interval(), Duration::from_millis(5000));
        cfg.poll_interval_ms = Some(10);
        assert_eq!(cfg.poll_interval(), Duration::from_millis(1000));
        cfg.poll_interval_ms = Some(2500);
        assert_eq!(cfg.poll_interval(), Duration::from_millis(2500));
    }

    #[test]
    fn turn_timeout_has_default_and_floor() {
        let mut cfg = ThreaditConfig::default();
        assert_eq!(cfg.turn_timeout(), Duration::from_secs(3600));
        cfg.turn_timeout_minutes = Some(0);
        assert_eq!(cfg.turn_timeout(), Duration::from_secs(60));
        cfg.turn_timeout_minutes = Some(5);
        assert_eq!(cfg.turn_timeout(), Duration::from_secs(300));
    }

    #[test]
    fn missing_token_error_names_variable_only() {
        let err = token_from(None).unwrap_err().to_string();
        assert!(err.contains("THREADIT_TOKEN"));
        assert!(token_from(Some("  ".into())).is_err());
        assert_eq!(token_from(Some(" abc ".into())).unwrap(), "abc");
    }

    #[test]
    fn activity_mode_parses_and_rejects_unknown() {
        let parse = |v: serde_json::Value| serde_json::from_value::<ThreaditConfig>(v);
        assert_eq!(
            parse(serde_json::json!({})).unwrap().activity,
            ActivityMode::Detailed
        );
        assert_eq!(
            parse(serde_json::json!({"activity": "detailed"}))
                .unwrap()
                .activity,
            ActivityMode::Detailed
        );
        assert_eq!(
            parse(serde_json::json!({"activity": "safe"}))
                .unwrap()
                .activity,
            ActivityMode::Safe
        );
        assert!(parse(serde_json::json!({"activity": "verbose"})).is_err());
        assert!(parse(serde_json::json!({"activity": "Detailed"})).is_err());
    }

    #[test]
    fn unknown_config_key_is_rejected() {
        let value = serde_json::json!({"base_url": "http://x", "tokn": "s3cret"});
        let err = serde_json::from_value::<ThreaditConfig>(value)
            .unwrap_err()
            .to_string();
        assert!(!err.contains("s3cret"));
    }
}
