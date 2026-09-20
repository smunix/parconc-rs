//! Central Server actor coordinating local clients and remote cluster nodes.
//!
//! Corresponds to `Server`, `checkAddClient`, `deleteClient`, `broadcast`,
//! `tell`, `kick`, and `handleRemoteMessage` in `chat.hs`.
//!
//! Maintains:
//! - Local connected clients: mapping `ClientName -> ClientId` (forwarding to `clients` group).
//! - Remote connected clients: mapping `ClientName -> ()` (or tracking existence across nodes).
//!
//! Dispatches:
//! - Local broadcasts to all local client actors via `ctx.send(DeliverToClient { client_id, .. })`.
//! - Remote broadcasts / new client notices / kicks across cluster nodes via `system.network`.

use elfo::{prelude::*, time::Interval};
use std::{collections::HashMap, time::Duration};
use tracing::{info, warn};

use crate::{
    client::{
        BroadcastRequest, ClientId, DeliverToClient, KickClient, KickRequest, RegisterClient,
        TellRequest, UnregisterClient,
    },
    protocol::{
        ChatMessage, ClientName, ClusterBroadcast, ClusterClientDisconnected, ClusterKick,
        ClusterNewClient, ClusterSend, ClusterSync,
    },
};

/// Internal tick to periodically announce local clients to the cluster for state reconciliation.
#[message]
struct SyncTick;

/// Represents a known client in the server's directory.
#[derive(Debug)]
enum ClientEntry {
    /// Connected directly to this local node's TCP server (identified by `ClientId`).
    Local(ClientId),
    /// Connected to another node in the cluster.
    Remote,
}

pub fn blueprint() -> Blueprint {
    ActorGroup::new().exec(|mut ctx| async move {
        // Table of all clients currently known to the cluster: local and remote.
        let mut clients: HashMap<ClientName, ClientEntry> = HashMap::new();

        // Periodically sync local client list across the cluster (every 3 seconds)
        ctx.attach(Interval::new(SyncTick)).start(Duration::from_secs(3));


        while let Some(envelope) = ctx.recv().await {
            msg!(match envelope {
                // -------------------------------------------------------------
                // Local Client Management (from client actors on this node)
                // -------------------------------------------------------------

                (RegisterClient { client_id, name }, token) => {
                    if clients.contains_key(&name) {
                        ctx.respond(token, Err(format!("The name '{name}' is in use, please choose another.")));
                    } else {
                        clients.insert(name.clone(), ClientEntry::Local(client_id));
                        ctx.respond(token, Ok(()));

                        info!(user = %name, client_id, "Local client registered");

                        // 1. Notify local clients
                        let notice = ChatMessage::Notice(format!("{name} has connected"));
                        for entry in clients.values() {
                            if let ClientEntry::Local(id) = entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }

                        // 2. Notify other nodes in the cluster
                        let _ = ctx.send(ClusterNewClient { name: name.clone() }).await;
                    }
                }

                UnregisterClient { client_id: _, name } => {
                    if let Some(ClientEntry::Local(_)) = clients.remove(&name) {
                        info!(user = %name, "Local client disconnected");

                        // 1. Notify local clients
                        let notice = ChatMessage::Notice(format!("{name} has disconnected"));
                        for entry in clients.values() {
                            if let ClientEntry::Local(id) = entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }

                        // 2. Notify remote nodes
                        let _ = ctx.send(ClusterClientDisconnected { name }).await;
                    }
                }

                BroadcastRequest { from, msg } => {
                    let chat_msg = ChatMessage::Broadcast {
                        from: from.clone(),
                        msg: msg.clone(),
                    };

                    // Deliver to all local clients
                    for entry in clients.values() {
                        if let ClientEntry::Local(id) = entry {
                            let _ = ctx.send(DeliverToClient {
                                client_id: *id,
                                msg: chat_msg.clone(),
                            }).await;
                        }
                    }

                    // Propagate to remote nodes
                    let _ = ctx.send(ClusterBroadcast { msg: chat_msg }).await;
                }

                (TellRequest { from, to, msg }, token) => {
                    match clients.get(&to) {
                        None => {
                            ctx.respond(token, Err(format!("{to} is not connected.")));
                        }
                        Some(ClientEntry::Local(target_id)) => {
                            let chat_msg = ChatMessage::Tell {
                                from: from.clone(),
                                msg: msg.clone(),
                            };
                            let _ = ctx.send(DeliverToClient {
                                client_id: *target_id,
                                msg: chat_msg.clone(),
                            }).await;

                            // Echo back to sender
                            if let Some(ClientEntry::Local(sender_id)) = clients.get(&from) {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *sender_id,
                                    msg: chat_msg,
                                }).await;
                            }
                            ctx.respond(token, Ok(()));
                        }
                        Some(ClientEntry::Remote) => {
                            let chat_msg = ChatMessage::Tell {
                                from: from.clone(),
                                msg: msg.clone(),
                            };
                            // Forward whisper across cluster
                            let _ = ctx.send(ClusterSend {
                                to: to.clone(),
                                msg: chat_msg.clone(),
                            }).await;

                            // Echo back to sender
                            if let Some(ClientEntry::Local(sender_id)) = clients.get(&from) {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *sender_id,
                                    msg: chat_msg,
                                }).await;
                            }
                            ctx.respond(token, Ok(()));
                        }
                    }
                }

                (KickRequest { kicker, victim }, token) => {
                    match clients.get(&victim) {
                        None => {
                            ctx.respond(token, Err(format!("{victim} is not connected.")));
                        }
                        Some(ClientEntry::Local(target_id)) => {
                            let _ = ctx.send(KickClient {
                                client_id: *target_id,
                                reason: format!("kicked by {kicker}"),
                            }).await;
                            ctx.respond(token, Ok(()));
                        }
                        Some(ClientEntry::Remote) => {
                            let _ = ctx.send(ClusterKick {
                                victim: victim.clone(),
                                by: kicker.clone(),
                            }).await;
                            ctx.respond(token, Ok(()));
                        }
                    }
                }

                // -------------------------------------------------------------
                // Internode Cluster Events (from remote nodes via elfo-network)
                // -------------------------------------------------------------

                ClusterNewClient { name } => {
                    if let Some(existing) = clients.get(&name) {
                        match existing {
                            ClientEntry::Local(local_id) => {
                                // Conflict: Name collision across nodes!
                                // The Haskell implementation resolves conflicts by kicking the duplicate.
                                warn!(name = %name, "Name conflict detected with remote node. Kicking duplicate.");
                                let _ = ctx.send(KickClient {
                                    client_id: *local_id,
                                    reason: "duplicate name on network".to_string(),
                                }).await;
                                let _ = ctx.send(ClusterKick {
                                    victim: name.clone(),
                                    by: "SYSTEM".to_string(),
                                }).await;
                            }
                            ClientEntry::Remote => {
                                // Already recorded as remote
                            }
                        }
                    } else {
                        info!(user = %name, "Remote client joined cluster");
                        clients.insert(name.clone(), ClientEntry::Remote);
                        let notice = ChatMessage::Notice(format!("{name} has connected"));
                        for entry in clients.values() {
                            if let ClientEntry::Local(id) = entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }
                    }
                }

                ClusterClientDisconnected { name } => {
                    if let Some(ClientEntry::Remote) = clients.remove(&name) {
                        info!(user = %name, "Remote client disconnected from cluster");
                        let notice = ChatMessage::Notice(format!("{name} has disconnected"));
                        for entry in clients.values() {
                            if let ClientEntry::Local(id) = entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: *id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }
                    }
                }

                ClusterBroadcast { msg } => {
                    // Forward broadcast from remote node to all local clients
                    for entry in clients.values() {
                        if let ClientEntry::Local(id) = entry {
                            let _ = ctx.send(DeliverToClient {
                                client_id: *id,
                                msg: msg.clone(),
                            }).await;
                        }
                    }
                }

                ClusterSend { to, msg } => {
                    // Directed whisper from remote node to our local client
                    if let Some(ClientEntry::Local(target_id)) = clients.get(&to) {
                        let _ = ctx.send(DeliverToClient {
                            client_id: *target_id,
                            msg,
                        }).await;
                    }
                }

                ClusterKick { victim, by } => {
                    // Remote node requested kicking someone
                    if let Some(ClientEntry::Local(target_id)) = clients.get(&victim) {
                        let _ = ctx.send(KickClient {
                            client_id: *target_id,
                            reason: format!("kicked by {by}"),
                        }).await;
                    }
                }

                ClusterSync { clients: remote_client_names } => {
                    for name in remote_client_names {
                        if !clients.contains_key(&name) {
                            info!(user = %name, "Discovered remote client via cluster sync");
                            clients.insert(name.clone(), ClientEntry::Remote);
                        }
                    }
                }

                SyncTick => {
                    let local_names: Vec<ClientName> = clients
                        .iter()
                        .filter_map(|(name, entry)| {
                            if let ClientEntry::Local(_) = entry {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !local_names.is_empty() {
                        let _ = ctx.send(ClusterSync { clients: local_names }).await;
                    }
                }
            });

        }
    })
}
