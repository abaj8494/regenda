use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

const DEFAULT_CONFIG_PATH: &str = "/opt/etc/reGenda/config.yml";

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub timezone: Option<String>,
    pub language: Option<String>,
    pub sources: HashMap<String, ServerConfig>,
}

/// A source can be either a CalDAV server (basic auth) or Google (OAuth 2.0).
#[derive(Deserialize, Clone, Debug)]
pub struct ServerConfig {
    #[serde(default = "default_type")]
    pub r#type: String,

    // CalDAV server fields (type: server)
    pub url: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,

    // Google OAuth fields (type: google)
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    /// Google calendar ID(s) to fetch. Defaults to "primary" (user's main calendar).
    pub calendar_id: Option<Vec<String>>,
}

fn default_type() -> String {
    "server".to_string()
}

impl ServerConfig {
    pub fn is_google(&self) -> bool {
        self.r#type == "google"
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = std::env::var("REGENDA_CONFIG")
            .unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());

        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file at {}", path))?;

        let config: Config = serde_yaml::from_str(&contents)
            .with_context(|| "Failed to parse config YAML")?;

        Ok(config)
    }

    pub fn timezone_str(&self) -> &str {
        self.timezone.as_deref().unwrap_or("UTC")
    }

    pub fn language_str(&self) -> &str {
        self.language.as_deref().unwrap_or("en")
    }

    /// Get all Google sources that need OAuth authorization.
    pub fn google_sources(&self) -> Vec<(&str, &ServerConfig)> {
        self.sources
            .iter()
            .filter(|(_, v)| v.is_google())
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }
}
