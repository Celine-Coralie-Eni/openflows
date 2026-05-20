use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::{mpsc, watch};
use anyhow::Result;

use crate::messages::{InboundMessage, OutboundMessage};

/// Pluggable channel abstraction for the Gateway.
#[async_trait]
pub trait ChannelPlugin: Send + Sync {
    /// Unique identifier for this channel type (e.g. "slack", "discord")
    fn channel_id(&self) -> &str;

    /// Start listening for inbound messages. Pushes to `tx`.
    /// Returns a shutdown handle.
    async fn start_listener(
        &self,
        tx: mpsc::Sender<InboundMessage>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>;

    /// Send an outbound message through this channel
    async fn send(&self, msg: &OutboundMessage) -> Result<()>;

    /// Ask a human a question and wait for response (optional)
    async fn ask_human(
        &self,
        question: &str,
        options: &[&str],
        ticket_id: &str,
        timeout_secs: u64,
    ) -> Option<String>;
}

/// Per-plugin config approach replacing monolithic ChatConfig.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GatewayConfig {
    pub enabled: bool,
    pub dev_mode: bool,
    pub channels: HashMap<String, serde_json::Value>,  // channel_id → config blob
}

impl GatewayConfig {
    pub fn from_env() -> Self {
        let mut channels = HashMap::new();

        // Slack
        if let Some(token) = std::env::var("NEXUS_GATEWAY_SLACK_BOT_TOKEN").ok() {
            let mut slack = serde_json::Map::new();
            slack.insert("bot_token".into(), token.into());
            if let Some(cid) = std::env::var("NEXUS_GATEWAY_SLACK_CHANNEL_ID").ok() {
                slack.insert("channel_id".into(), cid.into());
            }
            if let Some(secret) = std::env::var("NEXUS_GATEWAY_SLACK_SIGNING_SECRET").ok() {
                slack.insert("signing_secret".into(), secret.into());
            }
            channels.insert("slack".to_string(), slack.into());
        }

        // Discord
        if let Some(token) = std::env::var("NEXUS_GATEWAY_DISCORD_BOT_TOKEN").ok() {
            let mut discord = serde_json::Map::new();
            discord.insert("bot_token".into(), token.into());
            if let Some(cid) = std::env::var("NEXUS_GATEWAY_DISCORD_CHANNEL_ID").ok() {
                discord.insert("channel_id".into(), cid.into());
            }
            channels.insert("discord".to_string(), discord.into());
        }

        // WhatsApp
        if let Some(api_key) = std::env::var("NEXUS_GATEWAY_WHATSAPP_API_KEY").ok() {
            let mut wa = serde_json::Map::new();
            wa.insert("api_key".into(), api_key.into());
            if let Some(phone) = std::env::var("NEXUS_GATEWAY_WHATSAPP_PHONE_NUMBER").ok() {
                wa.insert("phone_number".into(), phone.into());
            }
            if let Some(url) = std::env::var("NEXUS_GATEWAY_WHATSAPP_API_URL").ok() {
                wa.insert("api_url".into(), url.into());
            }
            channels.insert("whatsapp".to_string(), wa.into());
        }

        Self {
            enabled: std::env::var("NEXUS_GATEWAY_ENABLED")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            dev_mode: std::env::var("NEXUS_GATEWAY_DEV_MODE")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            channels,
        }
    }

    pub fn is_configured(&self) -> bool {
        self.enabled && (self.dev_mode || !self.channels.is_empty())
    }

    pub fn active_channels(&self) -> Vec<String> {
        if self.dev_mode || self.channels.is_empty() {
            vec!["mock".to_string()]
        } else {
            self.channels.keys().cloned().collect()
        }
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dev_mode: false,
            channels: HashMap::new(),
        }
    }
}
