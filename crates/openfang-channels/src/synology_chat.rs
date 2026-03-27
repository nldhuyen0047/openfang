//! Synology Chat channel adapter.
//!
//! Integrates with Synology Chat via its webhook API:
//! - **Outgoing Webhook** (Synology → OpenFang): Synology Chat sends messages
//!   to our HTTP listener when users post in a configured channel.
//! - **Incoming Webhook** (OpenFang → Synology Chat): We POST responses back
//!   to Synology Chat's incoming webhook URL.
//!
//! ## Synology Chat Webhook Payload (Outgoing)
//!
//! Synology Chat sends POST requests with `application/x-www-form-urlencoded`
//! body containing a `payload` field with JSON:
//! ```json
//! {
//!   "token": "outgoing-webhook-token",
//!   "channel_id": 1,
//!   "channel_name": "general",
//!   "user_id": 1,
//!   "username": "alice",
//!   "post_id": "123456",
//!   "timestamp": "1700000000",
//!   "text": "Hello bot!"
//! }
//! ```
//!
//! ## Synology Chat Incoming Webhook
//!
//! We POST JSON to the incoming webhook URL:
//! ```json
//! {
//!   "text": "Response message"
//! }
//! ```
//! Optionally with `"file_url"` for file attachments.

use crate::types::{
    split_message, ChannelAdapter, ChannelContent, ChannelMessage, ChannelType, ChannelUser,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use url::form_urlencoded;
use zeroize::Zeroizing;

/// Synology Chat has a 65535 character limit per message.
const MAX_MESSAGE_LEN: usize = 65535;

/// Synology Chat channel adapter.
///
/// Uses Synology Chat's outgoing webhook to receive messages and incoming
/// webhook to send responses. Authentication is done via the outgoing webhook
/// token that Synology Chat includes in each request.
pub struct SynologyChatAdapter {
    /// The outgoing webhook token used to verify incoming requests from Synology Chat.
    outgoing_token: Zeroizing<String>,
    /// The incoming webhook URL to POST responses to.
    incoming_webhook_url: String,
    /// Port to listen for outgoing webhook POST requests from Synology Chat.
    listen_port: u16,
    /// HTTP client for sending responses.
    client: reqwest::Client,
    /// Shutdown signal.
    shutdown_tx: Arc<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
}

impl SynologyChatAdapter {
    /// Create a new Synology Chat adapter.
    ///
    /// # Arguments
    /// * `outgoing_token` - Token from Synology Chat outgoing webhook (for verification).
    /// * `incoming_webhook_url` - Synology Chat incoming webhook URL (for sending responses).
    /// * `listen_port` - Port to listen for outgoing webhook requests from Synology Chat.
    pub fn new(outgoing_token: String, incoming_webhook_url: String, listen_port: u16) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            outgoing_token: Zeroizing::new(outgoing_token),
            incoming_webhook_url,
            listen_port,
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            shutdown_tx: Arc::new(shutdown_tx),
            shutdown_rx,
        }
    }

    /// Parse the outgoing webhook payload from Synology Chat.
    ///
    /// Synology Chat sends `application/x-www-form-urlencoded` with a `payload`
    /// field containing JSON. Returns (text, user_id, username, post_id, channel_name).
    fn parse_outgoing_payload(
        payload_json: &serde_json::Value,
    ) -> Option<(String, String, String, String, String)> {
        let text = payload_json["text"].as_str()?.to_string();
        if text.is_empty() {
            return None;
        }

        let user_id = payload_json["user_id"]
            .as_u64()
            .map(|id| id.to_string())
            .or_else(|| payload_json["user_id"].as_str().map(String::from))
            .unwrap_or_else(|| "unknown".to_string());

        let username = payload_json["username"]
            .as_str()
            .unwrap_or("Synology User")
            .to_string();

        let post_id = payload_json["post_id"]
            .as_str()
            .or_else(|| payload_json["post_id"].as_u64().map(|_| "0"))
            .unwrap_or("0")
            .to_string();

        let channel_name = payload_json["channel_name"]
            .as_str()
            .unwrap_or("")
            .to_string();

        Some((text, user_id, username, post_id, channel_name))
    }
}

#[async_trait]
impl ChannelAdapter for SynologyChatAdapter {
    fn name(&self) -> &str {
        "synology_chat"
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Custom("synology_chat".to_string())
    }

    async fn start(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ChannelMessage> + Send>>, Box<dyn std::error::Error>>
    {
        let (tx, rx) = mpsc::channel::<ChannelMessage>(256);
        let port = self.listen_port;
        let outgoing_token = self.outgoing_token.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        info!("Synology Chat adapter starting HTTP server on port {port}");

        tokio::spawn(async move {
            let tx_shared = Arc::new(tx);
            let token_shared = Arc::new(outgoing_token);

            // Synology Chat sends outgoing webhooks as POST with form-encoded `payload` field
            let app = axum::Router::new().route(
                "/synology-chat",
                axum::routing::post({
                    let tx = Arc::clone(&tx_shared);
                    let expected_token = Arc::clone(&token_shared);
                    move |_headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let tx = Arc::clone(&tx);
                        let expected_token = Arc::clone(&expected_token);
                        async move {
                            info!("Synology Chat: received POST request ({} bytes)", body.len());
                            // Synology Chat sends application/x-www-form-urlencoded
                            // with a `payload` field containing JSON, or sometimes
                            // directly as JSON. We handle both.
                            let payload_json: serde_json::Value = {
                                // Parse form-encoded body to find the "payload" field.
                                // Synology Chat sends: payload=<url-encoded-json>
                                let parsed_form: Vec<(String, String)> =
                                    form_urlencoded::parse(&body)
                                        .map(|(k, v)| (k.into_owned(), v.into_owned()))
                                        .collect();

                                if let Some((_, json_str)) =
                                    parsed_form.iter().find(|(k, _)| k == "payload")
                                {
                                    // payload=<json> format (legacy)
                                    match serde_json::from_str(json_str) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            return (
                                                axum::http::StatusCode::BAD_REQUEST,
                                                "Invalid payload JSON",
                                            );
                                        }
                                    }
                                } else if !parsed_form.is_empty() {
                                    // Flat form-encoded format: token=xxx&text=hi&user_id=1&...
                                    let mut map = serde_json::Map::new();
                                    for (k, v) in &parsed_form {
                                        if let Ok(n) = v.parse::<u64>() {
                                            map.insert(k.clone(), serde_json::json!(n));
                                        } else {
                                            map.insert(k.clone(), serde_json::Value::String(v.clone()));
                                        }
                                    }
                                    serde_json::Value::Object(map)
                                } else {
                                    // Fallback: try direct JSON body
                                    match serde_json::from_slice(&body) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            return (
                                                axum::http::StatusCode::BAD_REQUEST,
                                                "Missing payload field",
                                            );
                                        }
                                    }
                                }
                            };

                            // Verify the outgoing webhook token
                            let token = payload_json["token"]
                                .as_str()
                                .unwrap_or("");
                            if token != expected_token.as_str() {
                                warn!("Synology Chat: invalid outgoing webhook token");
                                return (
                                    axum::http::StatusCode::FORBIDDEN,
                                    "Forbidden: invalid token",
                                );
                            }

                            if let Some((text, user_id, username, post_id, channel_name)) =
                                SynologyChatAdapter::parse_outgoing_payload(&payload_json)
                            {
                                // Prefix username so the agent knows who is speaking in group chat
                                let text_with_sender = format!("[{}]: {}", username, text);

                                let content = if text.starts_with('/') {
                                    let parts: Vec<&str> = text.splitn(2, ' ').collect();
                                    let cmd = parts[0].trim_start_matches('/');
                                    let args: Vec<String> = parts
                                        .get(1)
                                        .map(|a| {
                                            a.split_whitespace().map(String::from).collect()
                                        })
                                        .unwrap_or_default();
                                    ChannelContent::Command {
                                        name: cmd.to_string(),
                                        args,
                                    }
                                } else {
                                    ChannelContent::Text(text_with_sender)
                                };

                                let mut metadata = HashMap::new();
                                if !channel_name.is_empty() {
                                    metadata.insert(
                                        "channel_name".to_string(),
                                        serde_json::Value::String(channel_name),
                                    );
                                }
                                if let Some(channel_id) = payload_json["channel_id"].as_u64() {
                                    metadata.insert(
                                        "channel_id".to_string(),
                                        serde_json::json!(channel_id),
                                    );
                                }

                                let msg = ChannelMessage {
                                    channel: ChannelType::Custom("synology_chat".to_string()),
                                    platform_message_id: format!("syno-{post_id}"),
                                    sender: ChannelUser {
                                        platform_id: user_id,
                                        display_name: username,
                                        openfang_user: None,
                                    },
                                    content,
                                    target_agent: None,
                                    timestamp: Utc::now(),
                                    is_group: true, // Synology Chat webhooks are always from channels
                                    thread_id: None,
                                    metadata,
                                };

                                let _ = tx.send(msg).await;
                            }

                            (axum::http::StatusCode::OK, "ok")
                        }
                    }
                }),
            );

            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            info!("Synology Chat HTTP server listening on {addr}");

            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    warn!("Synology Chat: failed to bind port {port}: {e}");
                    return;
                }
            };

            let server = axum::serve(listener, app);

            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        warn!("Synology Chat server error: {e}");
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Synology Chat adapter shutting down");
                }
            }

            info!("Synology Chat HTTP server stopped");
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn send(
        &self,
        _user: &ChannelUser,
        content: ChannelContent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let text = match content {
            ChannelContent::Text(t) => t,
            _ => "(Unsupported content type)".to_string(),
        };

        let chunks = split_message(&text, MAX_MESSAGE_LEN);
        let num_chunks = chunks.len();

        for chunk in chunks {
            // Synology Chat incoming webhook expects JSON with "text" field
            // The text supports limited Markdown-like formatting
            let body = serde_json::json!({
                "text": chunk,
            });

            let body_str = serde_json::to_string(&body)?;

            // Synology Chat incoming webhook expects payload as form-encoded
            let form_body: String = form_urlencoded::Serializer::new(String::new())
                .append_pair("payload", &body_str)
                .finish();

            let resp = self
                .client
                .post(&self.incoming_webhook_url)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(form_body)
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status();
                let err_body = resp.text().await.unwrap_or_default();
                return Err(
                    format!("Synology Chat incoming webhook error {status}: {err_body}").into(),
                );
            }

            // Small delay between chunks for large messages
            if num_chunks > 1 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        Ok(())
    }

    async fn send_typing(&self, _user: &ChannelUser) -> Result<(), Box<dyn std::error::Error>> {
        // Synology Chat has no typing indicator API.
        Ok(())
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synology_chat_adapter_creation() {
        let adapter = SynologyChatAdapter::new(
            "test-token".to_string(),
            "https://nas.local:5001/webapi/entry.cgi?api=SYNO.Chat.External&method=incoming&version=2&token=%22abc%22".to_string(),
            8444,
        );
        assert_eq!(adapter.name(), "synology_chat");
        assert_eq!(
            adapter.channel_type(),
            ChannelType::Custom("synology_chat".to_string())
        );
    }

    #[test]
    fn test_parse_outgoing_payload_full() {
        let payload = serde_json::json!({
            "token": "outgoing-token",
            "channel_id": 1,
            "channel_name": "general",
            "user_id": 42,
            "username": "alice",
            "post_id": "123456",
            "timestamp": "1700000000",
            "text": "Hello bot!"
        });
        let result = SynologyChatAdapter::parse_outgoing_payload(&payload);
        assert!(result.is_some());
        let (text, user_id, username, post_id, channel_name) = result.unwrap();
        assert_eq!(text, "Hello bot!");
        assert_eq!(user_id, "42");
        assert_eq!(username, "alice");
        assert_eq!(post_id, "123456");
        assert_eq!(channel_name, "general");
    }

    #[test]
    fn test_parse_outgoing_payload_minimal() {
        let payload = serde_json::json!({
            "token": "tok",
            "text": "Just a message"
        });
        let result = SynologyChatAdapter::parse_outgoing_payload(&payload);
        assert!(result.is_some());
        let (text, user_id, username, _post_id, _channel_name) = result.unwrap();
        assert_eq!(text, "Just a message");
        assert_eq!(user_id, "unknown");
        assert_eq!(username, "Synology User");
    }

    #[test]
    fn test_parse_outgoing_payload_empty_text() {
        let payload = serde_json::json!({ "token": "tok", "text": "" });
        assert!(SynologyChatAdapter::parse_outgoing_payload(&payload).is_none());
    }

    #[test]
    fn test_parse_outgoing_payload_no_text() {
        let payload = serde_json::json!({ "token": "tok", "user_id": 1 });
        assert!(SynologyChatAdapter::parse_outgoing_payload(&payload).is_none());
    }

    #[test]
    fn test_parse_outgoing_payload_string_user_id() {
        let payload = serde_json::json!({
            "token": "tok",
            "text": "hi",
            "user_id": "user-str-123"
        });
        let result = SynologyChatAdapter::parse_outgoing_payload(&payload);
        assert!(result.is_some());
        let (_, user_id, _, _, _) = result.unwrap();
        assert_eq!(user_id, "user-str-123");
    }
}
