use crate::Failure;
use serde::Deserialize;
use serde_json::Value;
use std::path::{Component, Path};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub language: Option<String>,
    pub model_dir: String,
    pub keywords: Vec<String>,
    pub max_audio_secs: u64,
    pub max_file_mb: u64,
    pub timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            language: None,
            model_dir: "data/stt".into(),
            keywords: vec![],
            max_audio_secs: 900,
            max_file_mb: 25,
            timeout_secs: 600,
        }
    }
}

pub fn parse(value: Option<&Value>) -> Result<Option<Config>, Failure> {
    let Some(value) = value else { return Ok(None) };
    let mut cfg: Config = serde_json::from_value(value.clone()).map_err(|err| {
        Failure::new(
            "invalid_args",
            format!("Invalid transcribe configuration: {err}"),
        )
    })?;
    if cfg
        .language
        .as_deref()
        .is_some_and(|l| !["en", "de", "fr", "es", "it", "nl", "pl"].contains(&l))
    {
        return Err(Failure::new(
            "invalid_args",
            "language must be en, de, fr, es, it, nl or pl",
        ));
    }
    if cfg.model_dir.is_empty()
        || Path::new(&cfg.model_dir).is_absolute()
        || Path::new(&cfg.model_dir)
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(Failure::new(
            "invalid_args",
            "model_dir must be an agent-relative path without '..'",
        ));
    }
    if cfg.max_audio_secs == 0
        || cfg.timeout_secs == 0
        || cfg.max_file_mb == 0
        || cfg.max_file_mb.checked_mul(1024 * 1024).is_none()
    {
        return Err(Failure::new(
            "invalid_args",
            "Audio bounds must be positive and fit in bytes",
        ));
    }
    cfg.keywords = normalize_keywords(cfg.keywords)?;
    Ok(Some(cfg))
}

pub fn normalize_keywords(
    entries: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, Failure> {
    let mut result = Vec::new();
    for entry in entries {
        if entry.contains(['\n', '\r']) || entry.trim().is_empty() {
            return Err(Failure::new(
                "invalid_args",
                "Keywords must be non-empty single lines",
            ));
        }
        let entry = entry.trim().to_owned();
        if !result.contains(&entry) {
            result.push(entry);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn absent_is_inactive_but_present_section_uses_operator_defaults() {
        assert!(parse(None).unwrap().is_none());
        let cfg = parse(Some(&json!({}))).unwrap().unwrap();
        assert!(cfg.enabled);
        assert_eq!(
            (
                cfg.model_dir.as_str(),
                cfg.max_audio_secs,
                cfg.max_file_mb,
                cfg.timeout_secs
            ),
            ("data/stt", 900, 25, 600)
        );
        assert!(cfg.language.is_none());
        assert!(cfg.keywords.is_empty());
    }
    #[test]
    fn rejects_unsupported_language_and_escaping_model_directory() {
        for value in [
            json!({"language":"pt"}),
            json!({"model_dir":"/outside"}),
            json!({"model_dir":"data/../other"}),
            json!({"max_audio_secs":0}),
            json!({"timeout_secs":0}),
            json!({"max_file_mb":0}),
            json!({"unknown":true}),
        ] {
            assert!(parse(Some(&value)).is_err(), "accepted {value}");
        }
        assert!(
            !parse(Some(&json!({"enabled":false})))
                .unwrap()
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn keywords_are_trimmed_deduplicated_and_reject_line_injection() {
        let cfg = parse(Some(&json!({"keywords":[" Atlas ","Atlas","Élodie"]})))
            .unwrap()
            .unwrap();
        assert_eq!(cfg.keywords, ["Atlas", "Élodie"]);
        for keyword in ["", "  ", "Atlas\nBoreal", "Atlas\rBoreal"] {
            assert!(parse(Some(&json!({"keywords":[keyword]}))).is_err());
        }
    }
}
