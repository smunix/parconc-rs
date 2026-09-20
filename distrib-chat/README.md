# Distributed Chat Server in Rust using Elfo

A high-performance, fault-tolerant distributed chat server in Rust implemented using the [**elfo**](https://github.com/elfo-rs/elfo) actor framework.

This project is an idiomatic Rust port of the **Distributed Chat Server** problem described in Simon Marlow's book [***Parallel and Concurrent Programming in Haskell***](https://www.oreilly.com/library/view/parallel-and-concurrent/9781449335939/) (Chapter 12: *Distributed Programming with Cloud Haskell*, pages 262–270), originally implemented in `chat.hs`.

---

## 1. Problem Overview

The distributed chat problem simulates an IRC/Slack-like chat network spanning multiple distributed server instances across a cluster.

### Core Requirements
1. **Multi-Node Clustering**: Chat servers run as autonomous cluster nodes (e.g. Node 1, Node 2, Node 3) communicating over TCP.
2. **Network Compression**: Efficient internode frame transmission using native LZ4 compression negotiated transparently via `elfo-network`.
3. **Client Ingestion**: Users connect via standard telnet or netcat (`nc <host> <port>`) to *any* node in the cluster.
4. **Nickname Registration & Collision Handling**:
   - On connecting, the user is greeted with `What is your name?`.
   - If the chosen name is already taken by a client on *any* node in the cluster, registration is rejected (`The name '<name>' is in use, please choose another.`), prompting again.
5. **Directory / User Listing (`/users`, `/who`, `/list`)**:
   - Any connected client can list all other clients currently active across the cluster to discover who is available to message.
   - Output: `*** Connected users: Bob, Charlie` or `*** No other users are currently connected.`.
6. **Global Chat Broadcasts**:
   - Any message typed by a user on Node 1 is immediately relayed to all local clients on Node 1 *and* all remote clients on Node 2, Node 3, etc.
   - Format: `<Alice>: Hello world!`.
7. **Private Whispers (`/tell <user> <message>`)**:
   - Directed messaging routed specifically to the target client regardless of which node they are connected to.
   - If the target does not exist, the sender is informed (`<user> is not connected.`).
   - Format: `*Alice*: psst secret`.
8. **Administrative Kicks (`/kick <user>`)**:
   - Any user can kick another user anywhere in the cluster.
   - The victim is sent `You have been kicked: kicked by <kicker>` and their TCP connection is closed.
   - Kicker receives confirmation (`*** you kicked <victim>`).
   - Everyone receives a notice (`*** <victim> has disconnected`).
9. **Clean Disconnections (`/quit` or EOF)**:
   - When a client quits or terminates their connection, their name is released across the cluster and a broadcast notice is published (`*** <name> has disconnected`).

---

## 2. Architectural Comparison: Haskell vs. Rust (Elfo)

The Haskell implementation combines **Software Transactional Memory (STM)** for node-local state and **Cloud Haskell (`distributed-process`)** for internode actor messaging.

In Rust, **Elfo** provides a unified, actor-based architecture combining thread-safe in-memory message routing, sharded actor groups, and transparent node-to-node TCP clustering via `elfo-network`.

| Feature | Haskell (`chat.hs`) | Rust (`distrib-chat` with `elfo`) |
| :--- | :--- | :--- |
| **Node-Local State** | `TVar (Map ClientName Client)` with STM transactions | `server` actor group maintaining local & remote directory state without shared-memory locks |
| **TCP Client Handling** | Green thread per socket (`forkFinally (talk ...))`) | Sharded `clients` actor group with one actor per `ClientId` + attached async `Stream` reader |
| **Client Ingestion** | `socketListener` loop calling `accept` | Singleton `acceptor` actor listening on TCP, emitting `NewClientConnection` to spawn client actors |
| **User Directory** | TVar inspection | `ListUsersRequest` sent from client actor to central `server` actor |
| **Mailbox / Delivery** | `TChan Message` per local client | Dedicated actor mailbox per client actor; messages routed via `DeliverToClient` |
| **Internode Messaging** | Cloud Haskell `send pid (MsgSend ...)` | `elfo-network` TCP transport; `server` actor routes to `topology.remote("server")` |
| **Network Compression** | Uncompressed `Data.Binary` | Native `lz4_flex` frame compression negotiated during handshake (`compression.lz4 = "Preferred"`) |
| **Internode Protocol** | `data PMessage` with `Data.Binary` | Strongly-typed Rust structs with `#[message]` and `serde` |
| **Cluster Topology** | Master node collects `ProcessId`s and sends `MsgServers` | Decentralized mesh topology via TOML configuration (`discovery.predefined`) |
| **Failure Recovery** | Manual monitor/links or silent drop | Supervised actor groups, automatic TCP reconnection, and periodic `ClusterSync` state reconciliation |

---

## 3. Elfo Cluster Topology & Architecture

Each node runs an identical actor topology defined in [`src/main.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/main.rs):

```mermaid
flowchart TD
    subgraph Node1 ["Node 1 (e.g. port 44441)"]
        A1["acceptor actor<br/>(TCP 44441)"] -->|"NewClientConnection"| C1["clients group<br/>(sharded by ClientId)"]
        Telnet1["Telnet / nc (Alice)"] <==>|"TCP Stream"| C1
        C1 -->|"Register / ListUsers / Broadcast / Tell / Kick"| S1["server actor<br/>(Central Node Coordinator)"]
        S1 -->|"DeliverToClient / KickClient"| C1
        S1 <==>|"ClusterBroadcast / ClusterSend / ClusterKick"| Net1["system.network<br/>(elfo-network with LZ4)"]
    end

    subgraph Node2 ["Node 2 (e.g. port 44442)"]
        Net2["system.network<br/>(elfo-network with LZ4)"] <==>|"ClusterBroadcast / ClusterSend / ClusterKick"| S2["server actor<br/>(Central Node Coordinator)"]
        S2 -->|"DeliverToClient / KickClient"| C2["clients group<br/>(sharded by ClientId)"]
        A2["acceptor actor<br/>(TCP 44442)"] -->|"NewClientConnection"| C2
        Telnet2["Telnet / nc (Bob)"] <==>|"TCP Stream"| C2
    end

    Net1 <====="TCP Cluster Mesh with LZ4 (9301 <-> 9302)"=====> Net2
```

### Actor Groups Explained

1. **`acceptor` (Local Group)**:
   - Binds the TCP listener for chat clients (e.g., `0.0.0.0:44441`).
   - For every incoming TCP connection, assigns a unique `ClientId` and sends `NewClientConnection { client_id }` to the `clients` group.
2. **`clients` (Local Sharded Group)**:
   - Routed by `MapRouter` on `ClientId`. When `NewClientConnection` arrives for an unknown `ClientId`, Elfo automatically spawns a dedicated client actor.
   - Attaches an async `Stream::generate` reader to asynchronously read newline-delimited commands from the TCP socket.
   - Manages client authentication (`readName` protocol), command parsing (`/users`, `/tell`, `/kick`, `/quit`), and terminal writing.
3. **`server` (Local Group)**:
   - Central node directory. Tracks whether each known nickname is `Local(ClientId)` or `Remote`.
   - Handles `ListUsersRequest` to return sorted list of all active cluster clients.
   - Dispatches local deliveries, rejects duplicate names, and relays cluster events across `system.network`.
   - Periodically reconciles cluster state via `ClusterSync` ticks.
4. **`system.network` (Elfo Battery)**:
   - Handles TCP connection establishment, heartbeats (`Ping`/`Pong`), reconnect backoff, and transparent message serialization between cluster nodes.
   - Negotiates and executes LZ4 frame compression.
5. **`system.configurers` (Elfo Battery)**:
   - Dynamically loads and validates node configuration from TOML files.

---

## 4. Message Protocol

### Terminal / Local Client Messages ([`src/protocol.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/protocol.rs))
- `ChatMessage::Notice(String)`: System announcements (`*** Alice has connected`).
- `ChatMessage::Tell { from, msg }`: Whispers (`*Alice*: hello`).
- `ChatMessage::Broadcast { from, msg }`: Chat messages (`<Alice>: hello`).

### Cluster Synchronization Messages (Internode)
- `ClusterNewClient { name }`: Broadcast when a user registers on any node.
- `ClusterClientDisconnected { name }`: Broadcast when a user disconnects or quits.
- `ClusterBroadcast { msg }`: Relays a public chat message across all cluster nodes.
- `ClusterSend { to, msg }`: Routes a private message (whisper) across nodes to the destination client.
- `ClusterKick { victim, by }`: Directs the remote node hosting `victim` to terminate that client's connection.
- `ClusterSync { clients }`: Periodic cluster reconciliation ensuring convergence even after network hiccups.

---

## 5. Configuration & Compression

Network frame compression is activated by adding `compression.lz4 = "Preferred"` under `[system.network]` in each node's TOML file.

During initial node discovery and handshake, `elfo-network` exchanges capabilities and automatically enables LZ4 framing:
```
INFO system.network/server:... - connection picked up ... capabilities=caps(compression=LZ4)
```

### `config/node1.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9301"]
discovery.predefined = ["tcp://127.0.0.1:9302"]
compression.lz4 = "Preferred"
```

### `config/node2.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9302"]
discovery.predefined = ["tcp://127.0.0.1:9301"]
compression.lz4 = "Preferred"
```

### `config/node3.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9303"]
discovery.predefined = ["tcp://127.0.0.1:9301", "tcp://127.0.0.1:9302"]
compression.lz4 = "Preferred"
```

---

## 6. How to Build & Run

### Prerequisites
- Rust 1.85+ (Edition 2024)
- `telnet` or `netcat` (`nc`)

### 1. Build the Binary
```bash
cargo build --release
```

### 2. Start the Cluster Nodes
Open two (or three) separate terminal windows:

**Terminal 1 (Node 1):**
```bash
target/release/distrib-chat node1 44441
```

**Terminal 2 (Node 2):**
```bash
target/release/distrib-chat node2 44442
```

**Terminal 3 (Optional Node 3):**
```bash
target/release/distrib-chat node3 44443
```

You will see logs indicating that `system.network` has established LZ4-compressed data channels:
```
INFO system.network/server:33894:server - connection picked up ... capabilities=caps(compression=LZ4)
INFO acceptor/_ - Chat server listening for telnet clients addr=0.0.0.0:44441
```

---

## 7. Interactive Walkthrough

### Connecting Alice to Node 1
In another terminal:
```bash
nc 127.0.0.1 44441
```
Output:
```text
What is your name?
Alice
Welcome to the distributed chat, Alice!
Available commands: /users, /tell <user> <msg>, /kick <user>, /quit
*** Alice has connected
```

Querying other connected users while alone:
```text
/users
*** No other users are currently connected.
```

### Connecting Bob to Node 2
In another terminal:
```bash
nc 127.0.0.1 44442
```
Output:
```text
What is your name?
Bob
Welcome to the distributed chat, Bob!
Available commands: /users, /tell <user> <msg>, /kick <user>, /quit
*** Bob has connected
```

**Back on Alice's terminal (Node 1)**, Alice instantly receives:
```text
*** Bob has connected
```

Now Alice queries `/users`:
```text
/users
*** Connected users: Bob
```
And Bob queries `/users`:
```text
/users
*** Connected users: Alice
```

### Global Broadcast
In Bob's terminal:
```text
Hello from Node 2!
```
Alice on Node 1 receives:
```text
<Bob>: Hello from Node 2!
```

### Private Whisper Across Nodes
In Alice's terminal:
```text
/tell Bob secret message across nodes
```
- Alice sees: `*Alice*: secret message across nodes`
- Bob on Node 2 sees: `*Alice*: secret message across nodes`

### Duplicate Nickname Rejection
Try connecting a third client to Node 2 and entering `Alice`:
```text
What is your name?
Alice
The name 'Alice' is in use, please choose another.
What is your name?
```

### Kicking Across Nodes
In Alice's terminal:
```text
/kick Bob
```
- Alice sees:
  ```text
  *** You kicked Bob
  *** Bob has disconnected
  ```
- Bob's terminal sees:
  ```text
  You have been kicked: kicked by Alice
  ```
  *(and Bob's connection is immediately terminated)*

---

## 8. Automated Integration Test

To verify the entire cluster workflow programmatically, run:

```bash
cargo test --test cluster_test -- --nocapture
```

---

## 9. Reflection: Designing End-to-End Encryption (E2EE)

In the current architecture, private whispers (`/tell`) are protected by node-to-node transport if TLS is configured, but the **servers/cluster nodes themselves have complete visibility into the plaintext** of whispers when routing `ChatMessage::Tell` and `ClusterSend`.

Implementing true **End-to-End Encryption (E2EE)** guarantees that even compromised, untrusted, or rogue cluster nodes cannot inspect or tamper with private communications between two clients.

### 1. Threat Model & Design Goals
- **Untrusted Cluster**: Intermediate nodes must act only as blind routing relays.
- **Confidentiality**: Only the intended recipient can decrypt the message content.
- **Integrity & Authenticity**: Messages cannot be forged or tampered with by nodes or external attackers without detection.
- **Forward Secrecy**: Compromising a client's long-term key at time $T$ does not decrypt past messages sent before $T$.
- **Post-Compromise Security (Self-Healing)**: Once an attacker loses access, the session automatically heals and restores confidentiality.

### 2. Cryptographic Primitives & Key Exchange
To achieve state-of-the-art E2EE (analogous to Signal or Matrix), the system would implement:

1. **Identity & Ephemeral Keys**:
   - Each client generates an Identity Key Pair ($IK$) using **Ed25519** (for signatures) and **X25519** (for Diffie-Hellman).
   - Each client generates pre-signed ephemeral keys ($SPK$) and one-time prekeys ($OPK$).
2. **Key Directory on Server**:
   - The central `server` actor group stores public key bundles for each registered `ClientName`:
     ```rust
     struct ClientKeyBundle {
         identity_key: [u8; 32],
         signed_prekey: [u8; 32],
         prekey_signature: [u8; 64],
     }
     ```
   - Clients publish their public keys during registration (`RegisterClientWithKeys`).
   - A new command `/keys <user>` or automatic key fetch allows Alice to obtain Bob's public key bundle.
3. **Session Initialization via X3DH**:
   - When Alice wants to whisper to Bob, her client executes the **Extended Triple Diffie-Hellman (X3DH)** protocol:
     $$\text{SharedSecret} = \text{KDF}(DH(IK_A, SPK_B) \parallel DH(EK_A, IK_B) \parallel DH(EK_A, SPK_B))$$
   - Alice creates a session state and sends an initial encrypted handshake message.

### 3. Symmetric Encryption & The Double Ratchet
Once the session is established:
- Alice and Bob use the **Double Ratchet Algorithm**:
  - **DH Ratchet**: Every message exchange includes a new ephemeral public key, deriving fresh ratchet root keys.
  - **Symmetric-Key KDF Ratchet**: Derives a unique message encryption key per message.
- **AEAD Cipher**: **ChaCha20-Poly1305** or **AES-256-GCM** encrypts the payload, providing authenticated encryption with associated data (AEAD).

### 4. Protocol & Message Flow Changes
The protocol in `protocol.rs` would evolve from plain strings to encrypted envelopes:

```rust
#[message(part)]
pub struct EncryptedWhisperPayload {
    pub sender_ephemeral_key: [u8; 32],
    pub sequence_number: u32,
    pub previous_chain_length: u32,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
    pub tag: [u8; 16],
}

#[message]
pub struct ClusterSendEncrypted {
    pub to: ClientName,
    pub from: ClientName,
    pub payload: EncryptedWhisperPayload,
}
```

- When Node 1 receives `/tell Bob secret` from Alice:
  1. Alice's client encrypts `"secret"` into `EncryptedWhisperPayload`.
  2. Node 1 only sees recipient `to: "Bob"` and opaque ciphertext bytes.
  3. Node 1 routes `ClusterSendEncrypted` to Node 2 via `elfo-network`.
  4. Node 2 delivers the payload to Bob's client actor.
  5. Bob's client actor verifies AEAD authentication and decrypts using its local Double Ratchet state.
  6. Neither Node 1 nor Node 2 ever holds the decryption key.

### 5. Architectural Considerations: Fat Client vs. Server Proxy Actor
There are two potential deployment models for E2EE:

1. **Client-Side Terminal App (Pure E2EE)**:
   - The user runs a dedicated terminal CLI client (e.g. built with `ratatui` + `x25519-dalek`).
   - All cryptographic keys remain strictly on the user's local machine; the server never receives private keys.
   - Raw telnet cannot easily perform X25519/ChaCha20 operations, so a dedicated client binary or TLS client with E2EE extension is required.
2. **Actor-Mediated Zero-Trust Whispers**:
   - If clients connect via plain telnet, the client actor in `clients` can manage local session keys, but this places trust in the local node hosting that client actor (trusted ingress node, untrusted cluster network).
   - For true E2EE against compromised nodes, the cryptographic boundary must terminate inside the client software on the end-user machine.
