//! Protocol definitions for the distributed chat server.
//!
//! Corresponds to `Message` and `PMessage` in the Haskell implementation (`chat.hs`):
//! - Client chat messages: Notices, Whispers/Tells, and Broadcasts.
//! - Internode synchronization: Registering new clients, disconnects, whispers, and kicks across nodes.
//!
//! In `elfo`, types annotated with `#[message]` automatically derive `Serialize`,
//! `Deserialize`, `Debug`, and `Clone`, allowing transparent serialization
//! across cluster nodes via `elfo-network`.

use elfo::prelude::*;

/// Type alias for client nickname / username.
pub type ClientName = String;

// -----------------------------------------------------------------------------
// Client Chat Messages (Delivered to TCP client sockets)
// -----------------------------------------------------------------------------

/// Messages formatted and sent to the client's terminal / TCP connection.
/// Corresponds to Haskell's `data Message = Notice ... | Tell ... | Broadcast ...`
#[message(part)]
pub enum ChatMessage {
    /// System notices, e.g. "*** Alice has connected", "*** Bob was kicked"
    Notice(String),
    /// Private messages / whispers: "*Alice*: hello"
    Tell { from: ClientName, msg: String },
    /// Public chat broadcasts: "<Alice>: hello everyone"
    Broadcast { from: ClientName, msg: String },
}

impl ChatMessage {
    /// Formats the chat message for display in a telnet / TCP client terminal.
    pub fn format(&self) -> String {
        match self {
            ChatMessage::Notice(msg) => format!("*** {msg}\r\n"),
            ChatMessage::Tell { from, msg } => format!("*{from}*: {msg}\r\n"),
            ChatMessage::Broadcast { from, msg } => format!("<{from}>: {msg}\r\n"),
        }
    }
}

// -----------------------------------------------------------------------------
// Cluster Synchronization Protocol (Exchanged between nodes via elfo-network)
// -----------------------------------------------------------------------------

/// Sent across nodes when a new client connects and claims a username.
///
/// In Haskell: `MsgNewClient ClientName ProcessId`
/// In elfo, each node server actor tracks the origin node of remote clients.
#[message]
pub struct ClusterNewClient {
    pub name: ClientName,
}

/// Sent across nodes when a client disconnects or quits.
///
/// In Haskell: `MsgClientDisconnected ClientName ProcessId`
#[message]
pub struct ClusterClientDisconnected {
    pub name: ClientName,
}

/// Broadcasts a chat message to all connected clients on all nodes.
///
/// In Haskell: `MsgBroadcast Message`
#[message]
pub struct ClusterBroadcast {
    pub msg: ChatMessage,
}

/// Routes a private message (whisper) destined for a client hosted on a remote node.
///
/// In Haskell: `MsgSend ClientName Message`
#[message]
pub struct ClusterSend {
    pub to: ClientName,
    pub msg: ChatMessage,
}

/// Requests kicking a user across the cluster.
///
/// In Haskell: `MsgKick ClientName ClientName` (victim, kicker)
#[message]
pub struct ClusterKick {
    pub victim: ClientName,
    pub by: ClientName,
}

/// Periodic or on-connect state synchronization of active client names across the cluster.
#[message]
pub struct ClusterSync {
    pub clients: Vec<ClientName>,
}
