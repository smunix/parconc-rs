//! Protocol definitions for the distributed chat server and E2EE messaging.
//!
//! Corresponds to `Message` and `PMessage` in the Haskell implementation (`chat.hs`),
//! extended with End-to-End Encryption (E2EE) primitives:
//! - Client chat messages: Notices, Whispers/Tells, Encrypted Whispers (E2EE), and Broadcasts.
//! - Internode synchronization: Registering clients with public keys, key directory sync,
//!   opaque encrypted routing, whispers, and kicks across nodes.

use elfo::prelude::*;

/// Type alias for client nickname / username.
pub type ClientName = String;

// -----------------------------------------------------------------------------
// Client Chat Messages (Delivered to TCP client sockets)
// -----------------------------------------------------------------------------

/// Messages formatted and sent to the client's terminal / TCP connection.
/// Corresponds to Haskell's `data Message = Notice ... | Tell ... | Broadcast ...`
/// extended with `EncryptedTell` for E2EE payloads.
#[message(part)]
pub enum ChatMessage {
    /// System notices, e.g. "*** Alice has connected", "*** Bob was kicked"
    Notice(String),
    /// Unencrypted private messages / whispers: "*Alice*: hello"
    Tell { from: ClientName, msg: String },
    /// End-to-End Encrypted private whisper containing opaque base64 AEAD ciphertext.
    /// Intermediate servers route this payload without ability to decrypt.
    EncryptedTell {
        from: ClientName,
        ciphertext: String,
    },
    /// Public chat broadcasts: "<Alice>: hello everyone"
    Broadcast { from: ClientName, msg: String },
}

impl ChatMessage {
    /// Formats the chat message for display in a telnet / TCP client terminal.
    pub fn format(&self) -> String {
        match self {
            ChatMessage::Notice(msg) => format!("*** {msg}\r\n"),
            ChatMessage::Tell { from, msg } => format!("*{from}*: {msg}\r\n"),
            ChatMessage::EncryptedTell { from, ciphertext } => {
                format!("*E2EE* {from}: {ciphertext}\r\n")
            }
            ChatMessage::Broadcast { from, msg } => format!("<{from}>: {msg}\r\n"),
        }
    }
}

// -----------------------------------------------------------------------------
// Cluster Synchronization Protocol (Exchanged between nodes via elfo-network)
// -----------------------------------------------------------------------------

/// Sent across nodes when a new client connects and claims a username,
/// optionally publishing their public X25519 identity key.
#[message]
pub struct ClusterNewClient {
    pub name: ClientName,
    pub pubkey: Option<String>,
}

/// Broadcast to update or register a client's public key across the cluster.
#[message]
pub struct ClusterClientKey {
    pub name: ClientName,
    pub pubkey: String,
}

/// Sent across nodes when a client disconnects or quits.
#[message]
pub struct ClusterClientDisconnected {
    pub name: ClientName,
}

/// Broadcasts a chat message to all connected clients on all nodes.
#[message]
pub struct ClusterBroadcast {
    pub msg: ChatMessage,
}

/// Routes a private message (plain or encrypted whisper) destined for a client hosted on a remote node.
#[message]
pub struct ClusterSend {
    pub to: ClientName,
    pub msg: ChatMessage,
}

/// Requests kicking a user across the cluster.
#[message]
pub struct ClusterKick {
    pub victim: ClientName,
    pub by: ClientName,
}

/// Client descriptor for cluster synchronization.
#[message(part)]
pub struct ClientInfo {
    pub name: ClientName,
    pub pubkey: Option<String>,
}

/// Periodic or on-connect state synchronization of active clients and public keys.
#[message]
pub struct ClusterSync {
    pub clients: Vec<ClientInfo>,
}
