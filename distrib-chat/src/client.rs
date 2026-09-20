//! Local client actor and internal connection management.
//!
//! Corresponds to `LocalClient`, `talk`, `runClient`, and `handleMessage` in `chat.hs`.
//! Each connected TCP client (e.g. from `telnet`, `nc`, or `distrib-chat-client`) is managed
//! by a dedicated actor instance within the `clients` actor group, indexed by a unique `ClientId`.
//!
//! State machine:
//! 1. Client connects via TCP.
//! 2. Prompt "What is your name?".
//! 3. Verify name uniqueness with the `server` actor (supporting optional initial public key).
//! 4. If name is taken, inform the user and ask again.
//! 5. If accepted, broadcast join notice to the cluster and stream commands:
//!    - `/tell <user> <message>` -> plain whisper
//!    - `/etell <user> <ciphertext>` -> opaque E2EE whisper (routed blindly by server)
//!    - `/pubkey <key>` -> register or update client's public key
//!    - `/getkey <user>` -> fetch public key of another user
//!    - `/users` -> list connected users with E2EE capability indicators
//!    - `/kick <user>` -> kick user
//!    - `/quit` -> disconnect
//!    - `<message>` -> public broadcast

use elfo::{
    prelude::*,
    routers::{MapRouter, Outcome},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Mutex,
};
use tracing::{info, warn};

use std::collections::HashMap;

use crate::protocol::{ChatMessage, ClientName};

/// Unique identifier for a local client session.
pub type ClientId = u64;

/// Shared table holding pending socket streams before their corresponding actor picks them up.
pub type PendingSockets = Arc<Mutex<HashMap<ClientId, TcpStream>>>;

/// Notifies the `clients` actor group that a new TCP client has connected.
#[message]
pub struct NewClientConnection {
    pub client_id: ClientId,
}

/// Command or line typed by a client in the TCP stream.
#[message]
pub struct ClientInput {
    pub client_id: ClientId,
    pub text: String,
}

/// Sent to a client actor when their TCP connection is closed.
#[message]
pub struct ClientClosed {
    pub client_id: ClientId,
}

/// Delivers a formatted chat message to this client's terminal.
#[message]
pub struct DeliverToClient {
    pub client_id: ClientId,
    pub msg: ChatMessage,
}

/// Notifies the client that they have been kicked.
#[message]
pub struct KickClient {
    pub client_id: ClientId,
    pub reason: String,
}

// -----------------------------------------------------------------------------
// Request / Response interactions with the central Server actor
// -----------------------------------------------------------------------------

/// Request sent by a new client actor to the server to register its chosen nickname and optional public key.
#[message(ret = Result<(), String>)]
pub struct RegisterClient {
    pub client_id: ClientId,
    pub name: ClientName,
    pub pubkey: Option<String>,
}

/// Request to register/update public key for an existing client.
#[message(ret = Result<(), String>)]
pub struct RegisterKeyRequest {
    pub name: ClientName,
    pub pubkey: String,
}

/// Request to retrieve the registered public key of a user.
#[message(ret = Result<String, String>)]
pub struct GetKeyRequest {
    pub target: ClientName,
}

/// Request sent to the server to unregister upon disconnection.
#[message]
pub struct UnregisterClient {
    pub client_id: ClientId,
    pub name: ClientName,
}

/// Request to broadcast a message to everyone in the chat.
#[message]
pub struct BroadcastRequest {
    pub from: ClientName,
    pub msg: String,
}

/// Request to whisper (unencrypted) to a specific user.
#[message(ret = Result<(), String>)]
pub struct TellRequest {
    pub from: ClientName,
    pub to: ClientName,
    pub msg: String,
}

/// Request to deliver an opaque E2EE whisper payload to a specific user.
#[message(ret = Result<(), String>)]
pub struct EncryptedTellRequest {
    pub from: ClientName,
    pub to: ClientName,
    pub ciphertext: String,
}

/// Request to kick a user.
#[message(ret = Result<(), String>)]
pub struct KickRequest {
    pub kicker: ClientName,
    pub victim: ClientName,
}

/// User info for `/users` response including E2EE capability.
#[message(part)]
pub struct UserInfo {
    pub name: ClientName,
    pub has_e2ee: bool,
}

/// Request sent by a client actor to the server to list all active clients across the cluster.
#[message(ret = Vec<UserInfo>)]
pub struct ListUsersRequest;

/// Creates a `Blueprint` for the TCP acceptor actor group.
///
/// Listens for client connections on `tcp_port`.
/// For each accepted connection, it stores the stream and sends `NewClientConnection`
/// to the `clients` actor group, causing Elfo to spawn a new client actor.
pub fn acceptor_blueprint(tcp_port: u16, pending_sockets: PendingSockets) -> Blueprint {
    ActorGroup::new().exec(move |ctx| {
        let pending = pending_sockets.clone();
        async move {
            let addr: std::net::SocketAddr = match format!("0.0.0.0:{tcp_port}").parse() {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!("Invalid TCP address: {e}");
                    return;
                }
            };

            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(err) => {
                    tracing::error!(%addr, "Failed to bind chat TCP listener: {err}");
                    return;
                }
            };
            tracing::info!(%addr, "Chat server listening for telnet/E2EE clients");

            static NEXT_CLIENT_ID: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);

            while let Ok((stream, peer_addr)) = listener.accept().await {
                let client_id = NEXT_CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(%peer_addr, client_id, "Accepted incoming client connection");
                pending.lock().await.insert(client_id, stream);
                let _ = ctx.send(NewClientConnection { client_id }).await;
            }
        }
    })
}

/// Creates a `Blueprint` for the sharded `clients` actor group.
///
/// When `NewClientConnection { client_id }` arrives, `MapRouter` routes to
/// `Outcome::Unicast(client_id)`, automatically spawning a dedicated actor for that client.
pub fn blueprint(pending_sockets: PendingSockets) -> Blueprint {
    ActorGroup::new()
        .router(MapRouter::new(|envelope| {
            msg!(match envelope {
                NewClientConnection { client_id } => Outcome::Unicast(*client_id),
                ClientInput { client_id, .. } => Outcome::Unicast(*client_id),
                ClientClosed { client_id } => Outcome::Unicast(*client_id),
                DeliverToClient { client_id, .. } => Outcome::Unicast(*client_id),
                KickClient { client_id, .. } => Outcome::Unicast(*client_id),
                _ => Outcome::Default,
            })
        }))
        .exec(move |mut ctx: Context<(), ClientId>| {
            let pending_sockets = pending_sockets.clone();

            async move {
                let client_id = *ctx.key();

                // Retrieve the TCP socket associated with this client_id
                let stream = {
                    let mut lock = pending_sockets.lock().await;
                    lock.remove(&client_id)
                };

                let Some(socket) = stream else {
                    return;
                };

                let (mut read_half, write_half) = socket.into_split();
                let writer = Arc::new(Mutex::new(write_half));

                use elfo::stream::Stream;

                // Attach background reader stream for this client's TCP socket
                let reader_stream = Stream::generate(move |mut emitter| async move {
                    let mut reader = BufReader::new(&mut read_half);
                    let mut line = String::new();

                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) => {
                                // Connection closed (EOF)
                                let _ = emitter.emit(ClientClosed { client_id }).await;
                                break;
                            }
                            Ok(_) => {
                                let trimmed = line.trim_end_matches(&['\r', '\n'][..]).to_string();
                                emitter.emit(ClientInput { client_id, text: trimmed }).await;
                            }
                            Err(err) => {
                                warn!(client_id, "TCP read error: {err}");
                                let _ = emitter.emit(ClientClosed { client_id }).await;
                                break;
                            }
                        }
                    }
                });
                ctx.attach(reader_stream);

                // Helper to write a line to the TCP client
                let write_line = |writer: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>, text: &str| {
                    let w = writer.clone();
                    let owned = text.to_string();
                    async move {
                        let mut lock = w.lock().await;
                        let _ = lock.write_all(owned.as_bytes()).await;
                        let _ = lock.flush().await;
                    }
                };

                // 1. Prompt for nickname (corresponds to `readName` in Haskell `chat.hs`)
                write_line(&writer, "What is your name?\r\n").await;

                let mut client_name: Option<ClientName> = None;

                while let Some(envelope) = ctx.recv().await {
                    let mut registered = false;

                    msg!(match envelope {
                        ClientInput { text: raw_name, .. } => {
                            let mut parts = raw_name.split_whitespace();
                            let chosen_name = parts.next().unwrap_or("").to_string();
                            let pubkey = parts.next().map(|s| s.to_string());

                            if chosen_name.is_empty() {
                                continue;
                            }

                            // Try registering with the server actor (including optional pubkey)
                            match ctx.request(RegisterClient {
                                client_id,
                                name: chosen_name.clone(),
                                pubkey,
                            }).resolve().await {
                                Ok(Ok(())) => {
                                    info!(name = %chosen_name, client_id, "Client successfully logged in");
                                    client_name = Some(chosen_name);
                                    registered = true;
                                }
                                Ok(Err(reason)) => {
                                    write_line(&writer, &format!("{reason}\r\nWhat is your name?\r\n")).await;
                                }
                                Err(err) => {
                                    write_line(&writer, &format!("Internal server error: {err}\r\n")).await;
                                    return;
                                }
                            }
                        }
                        ClientClosed { .. } => {
                            return;
                        }
                        DeliverToClient { msg, .. } => {
                            write_line(&writer, &msg.format()).await;
                        }
                        KickClient { reason, .. } => {
                            write_line(&writer, &format!("You have been kicked: {reason}\r\n")).await;
                            return;
                        }
                    });

                    if registered {
                        break;
                    }
                }

                let Some(name) = client_name else {
                    return;
                };

                write_line(
                    &writer,
                    &format!("Welcome to the distributed chat, {name}!\r\nAvailable commands: /users, /tell <user> <msg>, /etell <user> <ciphertext>, /pubkey <key>, /getkey <user>, /kick <user>, /quit\r\n"),
                ).await;

                // 2. Main message loop (corresponds to `runClient` & `handleMessage` in Haskell `chat.hs`)
                while let Some(envelope) = ctx.recv().await {
                    msg!(match envelope {
                        DeliverToClient { msg, .. } => {
                            write_line(&writer, &msg.format()).await;
                        }
                        KickClient { reason, .. } => {
                            write_line(&writer, &format!("You have been kicked: {reason}\r\n")).await;
                            break;
                        }
                        ClientInput { text: line, .. } => {
                            if line.is_empty() {
                                continue;
                            }

                            if line == "/quit" {
                                break;
                            } else if line == "/users" || line == "/who" || line == "/list" {
                                match ctx.request(ListUsersRequest).resolve().await {
                                    Ok(all_users) => {
                                        let other_users: Vec<String> = all_users
                                            .into_iter()
                                            .filter(|u| u.name != name)
                                            .map(|u| {
                                                if u.has_e2ee {
                                                    format!("{} [e2ee]", u.name)
                                                } else {
                                                    u.name
                                                }
                                            })
                                            .collect();
                                        if other_users.is_empty() {
                                            write_line(&writer, "*** No other users are currently connected.\r\n").await;
                                        } else {
                                            write_line(
                                                &writer,
                                                &format!("*** Connected users: {}\r\n", other_users.join(", ")),
                                            ).await;
                                        }
                                    }
                                    Err(err) => {
                                        write_line(&writer, &format!("*** Error retrieving users: {err}\r\n")).await;
                                    }
                                }
                            } else if let Some(rest) = line.strip_prefix("/pubkey ") {
                                let key = rest.trim();
                                if key.is_empty() {
                                    write_line(&writer, "Usage: /pubkey <base64_public_key>\r\n").await;
                                } else {
                                    match ctx.request(RegisterKeyRequest {
                                        name: name.clone(),
                                        pubkey: key.to_string(),
                                    }).resolve().await {
                                        Ok(Ok(())) => {
                                            write_line(&writer, "*** Public key registered successfully.\r\n").await;
                                        }
                                        Ok(Err(err)) => {
                                            write_line(&writer, &format!("*** {err}\r\n")).await;
                                        }
                                        Err(err) => {
                                            write_line(&writer, &format!("*** Error registering key: {err}\r\n")).await;
                                        }
                                    }
                                }
                            } else if let Some(rest) = line.strip_prefix("/getkey ") {
                                let target = rest.trim();
                                if target.is_empty() {
                                    write_line(&writer, "Usage: /getkey <user>\r\n").await;
                                } else {
                                    match ctx.request(GetKeyRequest {
                                        target: target.to_string(),
                                    }).resolve().await {
                                        Ok(Ok(pubkey)) => {
                                            write_line(&writer, &format!("*** KEY {target} {pubkey}\r\n")).await;
                                        }
                                        Ok(Err(err)) => {
                                            write_line(&writer, &format!("*** {err}\r\n")).await;
                                        }
                                        Err(err) => {
                                            write_line(&writer, &format!("*** Error retrieving key: {err}\r\n")).await;
                                        }
                                    }
                                }
                            } else if let Some(rest) = line.strip_prefix("/etell ") {
                                let mut parts = rest.splitn(2, ' ');
                                let target = parts.next().unwrap_or("");
                                let ciphertext = parts.next().unwrap_or("");

                                if target.is_empty() || ciphertext.is_empty() {
                                    write_line(&writer, "Usage: /etell <user> <base64_ciphertext>\r\n").await;
                                } else {
                                    match ctx.request(EncryptedTellRequest {
                                        from: name.clone(),
                                        to: target.to_string(),
                                        ciphertext: ciphertext.to_string(),
                                    }).resolve().await {
                                        Ok(Ok(())) => {
                                            // Whisper routed successfully
                                        }
                                        Ok(Err(err)) => {
                                            write_line(&writer, &format!("*** {err}\r\n")).await;
                                        }
                                        Err(err) => {
                                            write_line(&writer, &format!("*** Error: {err}\r\n")).await;
                                        }
                                    }
                                }
                            } else if let Some(rest) = line.strip_prefix("/tell ") {
                                let mut parts = rest.splitn(2, ' ');
                                let target = parts.next().unwrap_or("");
                                let msg = parts.next().unwrap_or("");

                                if target.is_empty() || msg.is_empty() {
                                    write_line(&writer, "Usage: /tell <user> <message>\r\n").await;
                                } else {
                                    match ctx.request(TellRequest {
                                        from: name.clone(),
                                        to: target.to_string(),
                                        msg: msg.to_string(),
                                    }).resolve().await {
                                        Ok(Ok(())) => {
                                            // Whisper sent successfully
                                        }
                                        Ok(Err(err)) => {
                                            write_line(&writer, &format!("*** {err}\r\n")).await;
                                        }
                                        Err(err) => {
                                            write_line(&writer, &format!("*** Error: {err}\r\n")).await;
                                        }
                                    }
                                }
                            } else if let Some(rest) = line.strip_prefix("/kick ") {
                                let victim = rest.trim();
                                if victim.is_empty() {
                                    write_line(&writer, "Usage: /kick <user>\r\n").await;
                                } else {
                                    match ctx.request(KickRequest {
                                        kicker: name.clone(),
                                        victim: victim.to_string(),
                                    }).resolve().await {
                                        Ok(Ok(())) => {
                                            write_line(&writer, &format!("*** You kicked {victim}\r\n")).await;
                                        }
                                        Ok(Err(err)) => {
                                            write_line(&writer, &format!("*** {err}\r\n")).await;
                                        }
                                        Err(err) => {
                                            write_line(&writer, &format!("*** Error: {err}\r\n")).await;
                                        }
                                    }
                                }
                            } else if line.starts_with('/') {
                                write_line(&writer, &format!("Unrecognised command: {line}\r\n")).await;
                            } else {
                                // Regular public broadcast
                                let _ = ctx.send(BroadcastRequest {
                                    from: name.clone(),
                                    msg: line,
                                }).await;
                            }
                        }
                        ClientClosed { .. } => {
                            break;
                        }
                    });
                }

                // 3. Disconnect cleanup (corresponds to `disconnectLocalClient` in Haskell `chat.hs`)
                info!(name = %name, client_id, "Client disconnecting");
                let _ = ctx.send(UnregisterClient { client_id, name }).await;
            }
        })
}
