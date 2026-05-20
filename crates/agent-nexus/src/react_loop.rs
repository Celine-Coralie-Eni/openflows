use anyhow::Result;
use pocketflow_core::SharedStore;
use tracing::{info, warn};

use nexus_gateway::{Gateway, ReActLoop};

use crate::command_executor::execute_system_command;

/// Process inbound messages from the Gateway using a ReAct loop.
///
/// This is a thin wrapper around `nexus_gateway::ReActLoop` that creates
/// the loop on-demand with an `AgentRunner` resolved from the registry.
pub async fn process_gateway_messages(
    gateway: &Gateway,
    store: &SharedStore,
    registry_path: &std::path::Path,
) -> Result<()> {
    // Create an AgentRunner for the ReAct loop
    let registry = config::Registry::load(registry_path)?;
    let model_backend = registry.get("nexus").and_then(|e| e.model_backend.clone());
    let github_token = registry.resolve_github_token("nexus")?;

    let runner = agent_client::AgentRunner::from_env_with_token(
        model_backend.as_deref(),
        &github_token,
    )
    .await?;

    let mut react_loop = ReActLoop::new(runner, 8);
    // Note: Gateway is used for Respond steps inside the loop
    // We take it temporarily and restore after processing

    while let Some(msg) = gateway.try_recv_inbound() {
        info!(
            user = %msg.user_id,
            channel = %msg.channel_id,
            text = %msg.text,
            "Processing inbound message via ReAct loop"
        );

        match react_loop.run(&msg, store, Some(gateway)).await {
            Ok(steps) => {
                info!(steps = steps.len(), "ReAct loop completed");
            }
            Err(e) => {
                warn!("ReAct loop failed: {}", e);
            }
        }

        // After ReAct, also execute any direct system commands via interpreter
        // as a fallback for messages the loop may not have fully processed.
        let mut interpreter = nexus_gateway::CommandInterpreter::new_pattern_only();
        if let Some(interpreted) = interpreter.interpret(&msg).await {
            if let Err(e) = execute_system_command(&interpreted.command, store).await {
                warn!("Command execution failed: {}", e);
            }
        }
    }

    Ok(())
}
