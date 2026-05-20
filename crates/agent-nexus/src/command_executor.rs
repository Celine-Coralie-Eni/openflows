use anyhow::Result;
use config::state::{KEY_TICKETS, KEY_WORKER_SLOTS};
use config::{Ticket, TicketStatus, WorkerSlot, WorkerStatus};
use pocketflow_core::SharedStore;
use serde_json::json;
use std::collections::HashMap;
use tracing::info;

use nexus_gateway::messages::SystemCommand;

/// Execute a system command against the SharedStore.
pub async fn execute_system_command(cmd: &SystemCommand, store: &SharedStore) -> Result<()> {
    match cmd {
        SystemCommand::PauseWorkflow { ticket_id } => {
            pause_ticket(store, ticket_id).await?;
        }
        SystemCommand::ResumeWorkflow { ticket_id } => {
            resume_ticket(store, ticket_id).await?;
        }
        SystemCommand::ApproveCommand { worker_id } => {
            pocketflow_core::command_gate::CommandGate::approve(store, worker_id).await?;
            info!(worker_id, "Approved by command executor");
        }
        SystemCommand::BlockAgent { worker_id, reason } => {
            block_worker(store, worker_id, reason).await?;
        }
        SystemCommand::RerouteAgent { from_worker, to_worker } => {
            reroute_work(store, from_worker, to_worker).await?;
        }
        SystemCommand::AnswerQuestion { ticket_id, answer } => {
            let response_key = format!("human_response:{}", ticket_id);
            store.set(&response_key, json!(answer)).await;
            info!(ticket_id, "Answer recorded by command executor");
        }
        SystemCommand::StatusQuery | SystemCommand::GeneralMessage { .. } => {
            // No-op at store level
        }
    }
    Ok(())
}

pub async fn pause_ticket(store: &SharedStore, ticket_id: &str) -> Result<()> {
    let mut tickets: Vec<Ticket> = store.get_typed(KEY_TICKETS).await.unwrap_or_default();
    if let Some(ticket) = tickets.iter_mut().find(|t| t.id == ticket_id) {
        let worker_id = match &ticket.status {
            TicketStatus::InProgress { worker_id } => worker_id.clone(),
            TicketStatus::Assigned { worker_id } => worker_id.clone(),
            _ => return Ok(()),
        };

        ticket.status = TicketStatus::AwaitingHuman {
            worker_id: worker_id.clone(),
            reason: "paused_by_human".to_string(),
            attempts: 0,
        };

        let mut slots: HashMap<String, WorkerSlot> =
            store.get_typed(KEY_WORKER_SLOTS).await.unwrap_or_default();
        if let Some(slot) = slots.get_mut(&worker_id) {
            slot.status = WorkerStatus::Suspended {
                ticket_id: ticket_id.to_string(),
                reason: "paused_by_human".to_string(),
                issue_url: ticket.issue_url.clone(),
            };
        }
        store.set(KEY_WORKER_SLOTS, json!(slots)).await;
        store.set(KEY_TICKETS, json!(tickets)).await;
        info!(ticket_id, "Paused by command executor");
    }
    Ok(())
}

pub async fn resume_ticket(store: &SharedStore, ticket_id: &str) -> Result<()> {
    let mut tickets: Vec<Ticket> = store.get_typed(KEY_TICKETS).await.unwrap_or_default();
    if let Some(ticket) = tickets.iter_mut().find(|t| t.id == ticket_id) {
        if let TicketStatus::AwaitingHuman { worker_id, .. } = &ticket.status {
            let worker_id = worker_id.clone();
            ticket.status = TicketStatus::Open;
            ticket.attempts = 0;

            let mut slots: HashMap<String, WorkerSlot> =
                store.get_typed(KEY_WORKER_SLOTS).await.unwrap_or_default();
            if let Some(slot) = slots.get_mut(&worker_id) {
                slot.status = WorkerStatus::Idle;
            }
            store.set(KEY_WORKER_SLOTS, json!(slots)).await;
        }
    }
    store.set(KEY_TICKETS, json!(tickets)).await;
    info!(ticket_id, "Resumed by command executor");
    Ok(())
}

pub async fn block_worker(store: &SharedStore, worker_id: &str, reason: &str) -> Result<()> {
    let mut slots: HashMap<String, WorkerSlot> =
        store.get_typed(KEY_WORKER_SLOTS).await.unwrap_or_default();
    if let Some(slot) = slots.get_mut(worker_id) {
        if let WorkerStatus::Assigned { ticket_id, .. }
            | WorkerStatus::Working { ticket_id, .. } = &slot.status
        {
            let mut tickets: Vec<Ticket> = store.get_typed(KEY_TICKETS).await.unwrap_or_default();
            if let Some(ticket) = tickets.iter_mut().find(|t| t.id == *ticket_id) {
                ticket.status = TicketStatus::AwaitingHuman {
                    worker_id: worker_id.to_string(),
                    reason: format!("blocked_by_human: {}", reason),
                    attempts: 0,
                };
                store.set(KEY_TICKETS, json!(tickets)).await;
            }
            slot.status = WorkerStatus::Suspended {
                ticket_id: ticket_id.clone(),
                reason: format!("blocked_by_human: {}", reason),
                issue_url: None,
            };
        }
    }
    store.set(KEY_WORKER_SLOTS, json!(slots)).await;
    info!(worker_id, "Blocked by command executor");
    Ok(())
}

pub async fn reroute_work(
    store: &SharedStore,
    from_worker: &str,
    to_worker: &str,
) -> Result<()> {
    let mut slots: HashMap<String, WorkerSlot> =
        store.get_typed(KEY_WORKER_SLOTS).await.unwrap_or_default();

    let ticket_id = if let Some(from_slot) = slots.get(from_worker) {
        match &from_slot.status {
            WorkerStatus::Assigned { ticket_id, .. }
            | WorkerStatus::Working { ticket_id, .. } => Some(ticket_id.clone()),
            _ => None,
        }
    } else {
        None
    };

    if let Some(ticket_id) = ticket_id {
        if let Some(from_slot) = slots.get_mut(from_worker) {
            from_slot.status = WorkerStatus::Idle;
        }
        if let Some(to_slot) = slots.get_mut(to_worker) {
            let mut tickets: Vec<Ticket> =
                store.get_typed(KEY_TICKETS).await.unwrap_or_default();
            if let Some(ticket) = tickets.iter_mut().find(|t| t.id == ticket_id) {
                ticket.status = TicketStatus::Assigned {
                    worker_id: to_worker.to_string(),
                };
                to_slot.status = WorkerStatus::Assigned {
                    ticket_id: ticket_id.clone(),
                    issue_url: ticket.issue_url.clone(),
                };
                store.set(KEY_TICKETS, json!(tickets)).await;
            }
        }
    }
    store.set(KEY_WORKER_SLOTS, json!(slots)).await;
    info!(from = from_worker, to = to_worker, "Rerouted by command executor");
    Ok(())
}
