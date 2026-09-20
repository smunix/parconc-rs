//! Central Server actor coordinating local clients and remote cluster nodes.
//!
//! Corresponds to `Server`, `checkAddClient`, `deleteClient`, `broadcast`,
//! `tell`, `kick`, and `handleRemoteMessage` in `chat.hs`, extended with:
//! - Blind E2EE whisper routing (`EncryptedTellRequest` and `ClusterSend`).
//! - Distributed public key directory (`RegisterKeyRequest`, `GetKeyRequest`, `ClusterClientKey`).
//!
//! Maintains:
//! - Local connected clients: mapping `ClientName -> ClientState` (entry: Local(ClientId), pubkey).
//! - Remote connected clients: mapping `ClientName -> ClientState` (entry: Remote, pubkey).
//!
//! Dispatches:
//! - Local broadcasts to all local client actors via `ctx.send(DeliverToClient { client_id, .. })`.
//! - Remote broadcasts / new client notices / kicks across cluster nodes via `system.network`.
//! - Opaque E2EE whispers routed without decrypting or inspecting content.

use elfo::{prelude::*, time::Interval};
use std::{collections::HashMap, time::Duration};
use tracing::{info, warn};

use crate::{
    client::{
        BroadcastRequest, ClientId, DeliverToClient, EncryptedTellRequest, GetKeyRequest,
        KickClient, KickRequest, ListUsersRequest, RegisterClient, RegisterKeyRequest, TellRequest,
        UnregisterClient, UserInfo,
    },
    protocol::{
        ChatMessage, ClientInfo, ClientName, ClusterBroadcast, ClusterClientDisconnected,
        ClusterClientKey, ClusterKick, ClusterNewClient, ClusterSend, ClusterSync,
    },
};

/// Internal tick to periodically announce local clients and public keys to the cluster for state reconciliation.
#[message]
struct SyncTick;

/// Represents the connection location of a known client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientEntry {
    /// Connected directly to this local node's TCP server (identified by `ClientId`).
    Local(ClientId),
    /// Connected to another node in the cluster.
    Remote,
}

/// Information tracked for each registered client.
#[derive(Debug, Clone)]
pub struct ClientState {
    pub entry: ClientEntry,
    pub pubkey: Option<String>,
}

pub fn blueprint() -> Blueprint {
    ActorGroup::new().exec(|mut ctx| async move {
        // Table of all clients currently known to the cluster: local and remote.
        let mut clients: HashMap<ClientName, ClientState> = HashMap::new();

        // Periodically sync local client list across the cluster (every 3 seconds)
        ctx.attach(Interval::new(SyncTick)).start(Duration::from_secs(3));

        while let Some(envelope) = ctx.recv().await {
            msg!(match envelope {
                // -------------------------------------------------------------
                // Local Client Management (from client actors on this node)
                // -------------------------------------------------------------

                (RegisterClient { client_id, name, pubkey }, token) => {
                    if clients.contains_key(&name) {
                        ctx.respond(token, Err(format!("The name '{name}' is in use, please choose another.")));
                    } else {
                        clients.insert(name.clone(), ClientState {
                            entry: ClientEntry::Local(client_id),
                            pubkey: pubkey.clone(),
                        });
                        ctx.respond(token, Ok(()));

                        info!(user = %name, client_id, has_key = pubkey.is_some(), "Local client registered");

                        // 1. Notify local clients
                        let notice = ChatMessage::Notice(format!("{name} has connected"));
                        for state in clients.values() {
                            if let ClientEntry::Local(id) = state.entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }

                        // 2. Notify other nodes in the cluster
                        let _ = ctx.send(ClusterNewClient {
                            name: name.clone(),
                            pubkey,
                        }).await;
                    }
                }

                (RegisterKeyRequest { name, pubkey }, token) => {
                    if let Some(state) = clients.get_mut(&name) {
                        state.pubkey = Some(pubkey.clone());
                        ctx.respond(token, Ok(()));
                        info!(user = %name, "Registered E2EE public key");

                        // Propagate key update to other nodes
                        let _ = ctx.send(ClusterClientKey {
                            name,
                            pubkey,
                        }).await;
                    } else {
                        ctx.respond(token, Err(format!("User '{name}' not found.")));
                    }
                }

                (GetKeyRequest { target }, token) => {
                    match clients.get(&target) {
                        Some(state) => match &state.pubkey {
                            Some(pk) => ctx.respond(token, Ok(pk.clone())),
                            None => ctx.respond(token, Err(format!("User '{target}' does not have an E2EE public key registered."))),
                        },
                        None => ctx.respond(token, Err(format!("User '{target}' is not connected."))),
                    }
                }

                UnregisterClient { client_id: _, name } => {
                    if let Some(state) = clients.remove(&name)
                        && let ClientEntry::Local(_) = state.entry
                    {
                        info!(user = %name, "Local client disconnected");

                        // 1. Notify local clients
                        let notice = ChatMessage::Notice(format!("{name} has disconnected"));
                        for other_state in clients.values() {
                            if let ClientEntry::Local(id) = other_state.entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: id,
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
                    for state in clients.values() {
                        if let ClientEntry::Local(id) = state.entry {
                            let _ = ctx.send(DeliverToClient {
                                client_id: id,
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
                        Some(target_state) => {
                            let chat_msg = ChatMessage::Tell {
                                from: from.clone(),
                                msg: msg.clone(),
                            };
                            match target_state.entry {
                                ClientEntry::Local(target_id) => {
                                    let _ = ctx.send(DeliverToClient {
                                        client_id: target_id,
                                        msg: chat_msg.clone(),
                                    }).await;

                                    // Echo back to sender
                                    if let Some(ClientState { entry: ClientEntry::Local(sender_id), .. }) = clients.get(&from) {
                                        let _ = ctx.send(DeliverToClient {
                                            client_id: *sender_id,
                                            msg: chat_msg,
                                        }).await;
                                    }
                                    ctx.respond(token, Ok(()));
                                }
                                ClientEntry::Remote => {
                                    // Forward whisper across cluster
                                    let _ = ctx.send(ClusterSend {
                                        to: to.clone(),
                                        msg: chat_msg.clone(),
                                    }).await;

                                    // Echo back to sender
                                    if let Some(ClientState { entry: ClientEntry::Local(sender_id), .. }) = clients.get(&from) {
                                        let _ = ctx.send(DeliverToClient {
                                            client_id: *sender_id,
                                            msg: chat_msg,
                                        }).await;
                                    }
                                    ctx.respond(token, Ok(()));
                                }
                            }
                        }
                    }
                }

                // Opaque E2EE Whisper Routing
                (EncryptedTellRequest { from, to, ciphertext }, token) => {
                    match clients.get(&to) {
                        None => {
                            ctx.respond(token, Err(format!("{to} is not connected.")));
                        }
                        Some(target_state) => {
                            let chat_msg = ChatMessage::EncryptedTell {
                                from: from.clone(),
                                ciphertext: ciphertext.clone(),
                            };

                            match target_state.entry {
                                ClientEntry::Local(target_id) => {
                                    let _ = ctx.send(DeliverToClient {
                                        client_id: target_id,
                                        msg: chat_msg,
                                    }).await;
                                    ctx.respond(token, Ok(()));
                                }
                                ClientEntry::Remote => {
                                    let _ = ctx.send(ClusterSend {
                                        to: to.clone(),
                                        msg: chat_msg,
                                    }).await;
                                    ctx.respond(token, Ok(()));
                                }
                            }
                        }
                    }
                }

                (KickRequest { kicker, victim }, token) => {
                    match clients.get(&victim) {
                        None => {
                            ctx.respond(token, Err(format!("{victim} is not connected.")));
                        }
                        Some(target_state) => match target_state.entry {
                            ClientEntry::Local(target_id) => {
                                let _ = ctx.send(KickClient {
                                    client_id: target_id,
                                    reason: format!("kicked by {kicker}"),
                                }).await;
                                ctx.respond(token, Ok(()));
                            }
                            ClientEntry::Remote => {
                                let _ = ctx.send(ClusterKick {
                                    victim: victim.clone(),
                                    by: kicker.clone(),
                                }).await;
                                ctx.respond(token, Ok(()));
                            }
                        }
                    }
                }

                (ListUsersRequest, token) => {
                    let mut user_list: Vec<UserInfo> = clients
                        .iter()
                        .map(|(name, state)| UserInfo {
                            name: name.clone(),
                            has_e2ee: state.pubkey.is_some(),
                        })
                        .collect();
                    user_list.sort_by(|a, b| a.name.cmp(&b.name));
                    ctx.respond(token, user_list);
                }

                // -------------------------------------------------------------
                // Internode Cluster Events (from remote nodes via elfo-network)
                // -------------------------------------------------------------

                ClusterNewClient { name, pubkey } => {
                    if let Some(existing) = clients.get(&name) {
                        match existing.entry {
                            ClientEntry::Local(local_id) => {
                                // Conflict: Name collision across nodes!
                                warn!(name = %name, "Name conflict detected with remote node. Kicking duplicate.");
                                let _ = ctx.send(KickClient {
                                    client_id: local_id,
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
                        info!(user = %name, has_key = pubkey.is_some(), "Remote client joined cluster");
                        clients.insert(name.clone(), ClientState {
                            entry: ClientEntry::Remote,
                            pubkey,
                        });
                        let notice = ChatMessage::Notice(format!("{name} has connected"));
                        for state in clients.values() {
                            if let ClientEntry::Local(id) = state.entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }
                    }
                }

                ClusterClientKey { name, pubkey } => {
                    if let Some(state) = clients.get_mut(&name) {
                        info!(user = %name, "Updated E2EE public key from remote node");
                        state.pubkey = Some(pubkey);
                    }
                }

                ClusterClientDisconnected { name } => {
                    if let Some(state) = clients.remove(&name)
                        && state.entry == ClientEntry::Remote
                    {
                        info!(user = %name, "Remote client disconnected from cluster");
                        let notice = ChatMessage::Notice(format!("{name} has disconnected"));
                        for other in clients.values() {
                            if let ClientEntry::Local(id) = other.entry {
                                let _ = ctx.send(DeliverToClient {
                                    client_id: id,
                                    msg: notice.clone(),
                                }).await;
                            }
                        }
                    }
                }

                ClusterBroadcast { msg } => {
                    // Forward broadcast from remote node to all local clients
                    for state in clients.values() {
                        if let ClientEntry::Local(id) = state.entry {
                            let _ = ctx.send(DeliverToClient {
                                client_id: id,
                                msg: msg.clone(),
                            }).await;
                        }
                    }
                }

                ClusterSend { to, msg } => {
                    // Directed whisper (plain or encrypted) from remote node to our local client
                    if let Some(state) = clients.get(&to)
                        && let ClientEntry::Local(target_id) = state.entry
                    {
                        let _ = ctx.send(DeliverToClient {
                            client_id: target_id,
                            msg,
                        }).await;
                    }
                }

                ClusterKick { victim, by } => {
                    // Remote node requested kicking someone
                    if let Some(state) = clients.get(&victim)
                        && let ClientEntry::Local(target_id) = state.entry
                    {
                        let _ = ctx.send(KickClient {
                            client_id: target_id,
                            reason: format!("kicked by {by}"),
                        }).await;
                    }
                }

                ClusterSync { clients: remote_client_infos } => {
                    for info in remote_client_infos {
                        clients.entry(info.name.clone())
                            .and_modify(|s| {
                                if s.pubkey.is_none() && info.pubkey.is_some() {
                                    s.pubkey = info.pubkey.clone();
                                }
                            })
                            .or_insert_with(|| {
                                info!(user = %info.name, has_key = info.pubkey.is_some(), "Discovered remote client via cluster sync");
                                ClientState {
                                    entry: ClientEntry::Remote,
                                    pubkey: info.pubkey,
                                }
                            });
                    }
                }

                SyncTick => {
                    let local_infos: Vec<ClientInfo> = clients
                        .iter()
                        .filter_map(|(name, state)| {
                            if let ClientEntry::Local(_) = state.entry {
                                Some(ClientInfo {
                                    name: name.clone(),
                                    pubkey: state.pubkey.clone(),
                                })
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !local_infos.is_empty() {
                        let _ = ctx.send(ClusterSync { clients: local_infos }).await;
                    }
                }
            });
        }
    })
}
