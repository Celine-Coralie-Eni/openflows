use anyhow::{bail, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{info, warn, debug};

use crate::messages::{InboundMessage, OutboundMessage, OutboundMessageType};
use crate::plugin::ChannelPlugin;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscordMessage {
    id: String,
    content: String,
    author_id: Option<String>,
    channel_id: String,
    message_reference: Option<String>,
}

pub struct DiscordPlugin {
    client: Client,
    bot_token: String,
    channel_id: String,
}

impl DiscordPlugin {
    pub fn new(bot_token: String, channel_id: String) -> Self {
        Self {
            client: Client::new(),
            bot_token,
            channel_id,
        }
    }

    pub fn from_config(config: &serde_json::Value) -> Option<Self> {
        let token = config.get("bot_token")?.as_str()?;
        let channel = config.get("channel_id")?.as_str()?;
        Some(Self::new(token.to_string(), channel.to_string()))
    }

    fn format_message(&self, msg: &OutboundMessage) -> String {
        match msg.message_type {
            OutboundMessageType::WorkflowStarted => {
                format!(
                    "🚀 Starting ticket {}: {}",
                    msg.ticket_id.as_deref().unwrap_or("?"),
                    msg.content
                )
            }
            OutboundMessageType::AgentAssigned => {
                format!(
                    "👷 {} assigned to {}",
                    msg.worker_id.as_deref().unwrap_or("?"),
                    msg.content
                )
            }
            OutboundMessageType::AgentCompleted => {
                format!(
                    "✅ {} completed: {}",
                    msg.worker_id.as_deref().unwrap_or("?"),
                    msg.content
                )
            }
            OutboundMessageType::WorkflowError => {
                format!(
                    "❌ {}: {}",
                    msg.worker_id.as_deref().unwrap_or("?"),
                    msg.content
                )
            }
            OutboundMessageType::QuestionToHuman => {
                format!("🤔 {}", msg.content)
            }
            OutboundMessageType::ApprovalRequest => {
                format!("⚠️ Approval needed: {}", msg.content)
            }
            OutboundMessageType::StatusUpdate => {
                format!("📊 {}", msg.content)
            }
            _ => msg.content.clone(),
        }
    }

    fn build_embeds(&self, msg: &OutboundMessage) -> Vec<serde_json::Value> {
        match msg.message_type {
            OutboundMessageType::ApprovalRequest => {
                vec![json!({
                    "title": "Approval Request",
                    "description": msg.content,
                    "color": 16753920,
                    "footer": {
                        "text": format!("Reply with `approve {}` to approve or `reject {}` to reject",
                            msg.worker_id.as_deref().unwrap_or("?"),
                            msg.worker_id.as_deref().unwrap_or("?"))
                    }
                })]
            }
            OutboundMessageType::QuestionToHuman => {
                let options = msg.metadata.get("options")
                    .and_then(|o| o.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|o| o.as_str())
                            .enumerate()
                            .map(|(i, o)| format!("{}. {}", i + 1, o))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                vec![json!({
                    "title": "Question",
                    "description": msg.content,
                    "color": 3447003,
                    "fields": [{
                        "name": "Options",
                        "value": options,
                        "inline": false
                    }],
                    "footer": {
                        "text": format!("Reply with `answer {}: <your response>`",
                            msg.ticket_id.as_deref().unwrap_or("?"))
                    }
                })]
            }
            _ => vec![],
        }
    }

    async fn send_to_discord(&self, msg: &OutboundMessage) -> Result<()> {
        let text = self.format_message(msg);
        let embeds = self.build_embeds(msg);

        let response = self
            .client
            .post(&format!(
                "https://discord.com/api/v10/channels/{}/messages",
                self.channel_id
            ))
            .header("Authorization", format!("Bot {}", self.bot_token))
            .header("Content-Type", "application/json")
            .json(&json!({
                "content": text,
                "embeds": embeds,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body: serde_json::Value = response.json().await?;
            bail!("Discord API error: {:?}", body);
        }

        info!(message_type = ?msg.message_type, "Sent Discord message");
        Ok(())
    }
}

#[async_trait]
impl ChannelPlugin for DiscordPlugin {
    fn channel_id(&self) -> &str {
        "discord"
    }

    async fn start_listener(
        &self,
        tx: mpsc::Sender<InboundMessage>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        run_discord_gateway(self.bot_token.clone(), self.channel_id.clone(), tx, shutdown).await
    }

    async fn send(&self, msg: &OutboundMessage) -> Result<()> {
        self.send_to_discord(msg).await
    }

    async fn ask_human(
        &self,
        question: &str,
        options: &[&str],
        ticket_id: &str,
        _timeout_secs: u64,
    ) -> Option<String> {
        let msg = OutboundMessage {
            message_type: OutboundMessageType::QuestionToHuman,
            target_channel: None,
            target_conversation: None,
            content: question.to_string(),
            ticket_id: Some(ticket_id.to_string()),
            worker_id: None,
            metadata: json!({"options": options}),
        };
        if self.send_to_discord(&msg).await.is_err() {
            return None;
        }
        None
    }
}

// ── Discord Gateway (WebSocket) ─────────────────────────────────────────────

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

// Discord Gateway intents: GUILD_MESSAGES = 1 << 9 = 512, MESSAGE_CONTENT = 1 << 15 = 32768
const INTENTS: u64 = 512 | 32768;
const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

#[derive(Debug, Clone, Deserialize)]
struct GatewayPayload {
    op: u8,
    #[serde(rename = "d")]
    data: Option<serde_json::Value>,
    #[serde(rename = "s")]
    sequence: Option<u64>,
    #[serde(rename = "t")]
    event_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct HelloData {
    heartbeat_interval: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ReadyData {
    user: GatewayUser,
}

#[derive(Debug, Clone, Deserialize)]
struct GatewayUser {
    id: String,
    username: String,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageCreateAuthor {
    id: String,
    username: String,
    #[serde(default)]
    bot: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct MentionUser {
    id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct MessageCreateData {
    id: String,
    content: String,
    channel_id: String,
    author: MessageCreateAuthor,
    #[serde(default)]
    mentions: Vec<MentionUser>,
}

async fn run_discord_gateway(
    token: String,
    target_channel: String,
    message_tx: mpsc::Sender<InboundMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    info!("Discord Gateway background task starting");
    let mut backoff_secs = 1u64;

    loop {
        if *shutdown_rx.borrow() {
            info!("Discord Gateway shutdown requested, exiting reconnect loop");
            break Ok(());
        }

        info!("Discord Gateway connecting (attempt)");
        match run_gateway_once(token.clone(), target_channel.clone(), message_tx.clone(), shutdown_rx.clone()).await {
            Ok(()) => {
                if *shutdown_rx.borrow() {
                    info!("Discord Gateway shut down gracefully");
                    break Ok(());
                }
                warn!("Discord Gateway connection closed, will reconnect");
            }
            Err(e) => {
                warn!("Discord Gateway connection error: {}", e);
                if *shutdown_rx.borrow() {
                    break Ok(());
                }
            }
        }

        warn!(seconds = backoff_secs, "Discord Gateway reconnecting with backoff");
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
            _ = shutdown_rx.changed() => {
                info!("Discord Gateway shutdown requested during backoff, exiting");
                break Ok(());
            }
        }
        backoff_secs = (backoff_secs * 2).min(60);
    }
}

async fn run_gateway_once(
    token: String,
    target_channel: String,
    message_tx: mpsc::Sender<InboundMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    info!("run_gateway_once: connecting to Discord Gateway");

    let (ws_stream, _) = connect_async(GATEWAY_URL).await?;
    let (mut ws_sink, mut ws_stream) = ws_stream.split();
    info!("run_gateway_once: WebSocket connected");

    let mut heartbeat_interval: u64 = 41250;
    let mut next_heartbeat = tokio::time::Instant::now()
        + std::time::Duration::from_millis(heartbeat_interval);
    let mut sequence: Option<u64> = None;
    let mut bot_user_id: Option<String> = None;
    let mut bot_username: Option<String> = None;

    #[derive(Debug)]
    enum ConnectionState {
        WaitingForHello,
        WaitingForReady,
        Connected,
    }
    let mut state = ConnectionState::WaitingForHello;

    loop {
        let now = tokio::time::Instant::now();
        let sleep_duration = next_heartbeat.saturating_duration_since(now);

        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Discord Gateway received shutdown signal");
                let _ = ws_sink.close().await;
                return Ok(());
            }

            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let payload: GatewayPayload = match serde_json::from_str(&text) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!("Failed to parse gateway payload: {}", e);
                                continue;
                            }
                        };

                        if payload.sequence.is_some() {
                            sequence = payload.sequence;
                        }

                        match payload.op {
                            1 => { // Heartbeat Request
                                let heartbeat = json!({ "op": 1, "d": sequence });
                                if let Err(e) = ws_sink.send(WsMessage::Text(heartbeat.to_string())).await {
                                    warn!("Failed to send requested heartbeat: {}", e);
                                } else {
                                    next_heartbeat = tokio::time::Instant::now()
                                        + std::time::Duration::from_millis(heartbeat_interval);
                                    info!("Immediate heartbeat sent to Discord Gateway");
                                }
                            }

                            10 => { // Hello
                                if let Some(data) = payload.data {
                                    if let Ok(hello) = serde_json::from_value::<HelloData>(data) {
                                        heartbeat_interval = hello.heartbeat_interval;
                                        let first_heartbeat_delay = heartbeat_interval.min(5000);
                                        next_heartbeat = tokio::time::Instant::now()
                                            + std::time::Duration::from_millis(first_heartbeat_delay);
                                        info!(interval = heartbeat_interval, "Discord Gateway Hello received");

                                        let identify = json!({
                                            "op": 2,
                                            "d": {
                                                "token": token,
                                                "intents": INTENTS,
                                                "properties": {
                                                    "os": "linux",
                                                    "browser": "nexus-gateway",
                                                    "device": "nexus-gateway"
                                                }
                                            }
                                        });

                                        ws_sink.send(WsMessage::Text(identify.to_string())).await?;
                                        info!("Sent Identify to Discord Gateway");
                                        state = ConnectionState::WaitingForReady;
                                    }
                                }
                            }

                            11 => { // Heartbeat ACK
                                debug!("Heartbeat ACK received");
                            }

                            0 => { // Event dispatch
                                if let Some(event_type) = &payload.event_type {
                                    match event_type.as_str() {
                                        "READY" => {
                                            if let Some(data) = payload.data.clone() {
                                                if let Ok(ready) = serde_json::from_value::<ReadyData>(data) {
                                                    bot_user_id = Some(ready.user.id.clone());
                                                    bot_username = Some(ready.user.username.clone());
                                                    info!(bot_id = %ready.user.id, bot_name = %ready.user.username, "Discord Gateway READY - connected as bot");
                                                    state = ConnectionState::Connected;
                                                }
                                            }
                                        }

                                        "MESSAGE_CREATE" => {
                                            if let Some(data) = payload.data.clone() {
                                                if let Ok(msg_data) = serde_json::from_value::<MessageCreateData>(data) {
                                                    if msg_data.channel_id == target_channel && !msg_data.author.bot {
                                                        let is_mentioned = bot_user_id.as_ref().map(|bot_id| {
                                                            msg_data.mentions.iter().any(|m| &m.id == bot_id)
                                                        }).unwrap_or(false);

                                                        let starts_with_bot = bot_username.as_ref().map(|name| {
                                                            let lower_content = msg_data.content.to_lowercase();
                                                            let lower_name = name.to_lowercase();
                                                            lower_content.starts_with(&lower_name)
                                                                && (lower_content.len() == lower_name.len()
                                                                    || lower_content[lower_name.len()..].starts_with(char::is_whitespace))
                                                        }).unwrap_or(false);

                                                        if is_mentioned || starts_with_bot {
                                                            let content = if starts_with_bot {
                                                                strip_prefix(&msg_data.content, bot_username.as_ref().unwrap())
                                                            } else if is_mentioned {
                                                                strip_mention(&msg_data.content, bot_user_id.as_ref().unwrap())
                                                            } else {
                                                                msg_data.content.clone()
                                                            };

                                                            let human_msg = InboundMessage {
                                                                message_id: msg_data.id,
                                                                channel_id: "discord".to_string(),
                                                                user_id: msg_data.author.id,
                                                                conversation_id: msg_data.channel_id,
                                                                text: content,
                                                                timestamp: chrono::Utc::now(),
                                                                metadata: serde_json::Value::Null,
                                                            };
                                                            if let Err(e) = message_tx.send(human_msg).await {
                                                                warn!("Failed to send message to channel: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        _ => { debug!(event = %event_type, "Unhandled gateway event"); }
                                    }
                                }
                            }

                            9 => { // Invalid session
                                warn!("Discord Gateway session invalidated");
                                return Err(anyhow::anyhow!("Discord session invalidated"));
                            }

                            7 => { // Reconnect request
                                warn!("Discord Gateway requesting reconnect");
                                return Err(anyhow::anyhow!("Discord requested reconnect"));
                            }

                            _ => { debug!(op = payload.op, "Unknown gateway opcode"); }
                        }
                    }

                    Some(Ok(WsMessage::Ping(data))) => {
                        let _ = ws_sink.send(WsMessage::Pong(data)).await;
                    }

                    Some(Ok(WsMessage::Close(Some(frame)))) => {
                        warn!(code = %frame.code, reason = %frame.reason, "Discord Gateway connection closed by server");
                        return Ok(());
                    }

                    Some(Ok(WsMessage::Close(None))) => {
                        warn!("Discord Gateway connection closed by server (no close frame)");
                        return Ok(());
                    }

                    Some(Ok(WsMessage::Pong(_))) => {
                        debug!("Pong received");
                    }

                    Some(Err(e)) => {
                        warn!("WebSocket error: {}", e);
                        return Err(anyhow::anyhow!("WebSocket error: {}", e));
                    }

                    None => {
                        warn!("WebSocket stream ended");
                        return Err(anyhow::anyhow!("WebSocket stream ended"));
                    }

                    _ => {}
                }
            }

            _ = tokio::time::sleep(sleep_duration) => {
                if matches!(state, ConnectionState::Connected | ConnectionState::WaitingForReady) {
                    let heartbeat = json!({ "op": 1, "d": sequence });
                    if let Err(e) = ws_sink.send(WsMessage::Text(heartbeat.to_string())).await {
                        warn!("Failed to send heartbeat: {}", e);
                        return Err(anyhow::anyhow!("Failed to send heartbeat: {}", e));
                    }
                    debug!("Heartbeat sent");
                    next_heartbeat = tokio::time::Instant::now()
                        + std::time::Duration::from_millis(heartbeat_interval);
                }
            }
        }
    }
}

/// Strip prefix from message content (case-insensitive).
fn strip_prefix(content: &str, prefix: &str) -> String {
    let lower = content.to_lowercase();
    let lower_prefix = prefix.to_lowercase();
    if lower.starts_with(&lower_prefix) {
        let prefix_chars = prefix.chars().count();
        let rest: String = content.chars().skip(prefix_chars).collect();
        rest.trim().to_string()
    } else {
        content.to_string()
    }
}

/// Strip Discord mention from message content.
fn strip_mention(content: &str, bot_id: &str) -> String {
    let patterns = vec![
        format!("<@{}>", bot_id),
        format!("<@!{}>", bot_id),
    ];
    let mut result = content.to_string();
    for pattern in patterns {
        result = result.replace(&pattern, "");
    }
    result.trim().to_string()
}
