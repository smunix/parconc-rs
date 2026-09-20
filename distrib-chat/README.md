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

## 8. Automated Integration Tests

To verify both standard cluster routing and the End-to-End Encryption (E2EE) pipeline programmatically, run:

```bash
# Run all automated integration and unit tests
cargo test -- --nocapture

# Run the dedicated cluster E2EE integration test
cargo test --test e2ee_test -- --nocapture

# Run the standard cluster synchronization test
cargo test --test cluster_test -- --nocapture
```

---

## 9. End-to-End Encryption (E2EE) & Dedicated Terminal Client

The distributed chat system features full **End-to-End Encryption (E2EE)** for private whispers between users across the cluster. Intermediate server nodes act purely as blind routing relays: they forward opaque base64 ciphertext payloads and have **zero visibility** into the plaintext contents, nor do they possess private keys.

### Cryptographic Stack & Primitives
- **Key Agreement (Diffie-Hellman)**: **X25519** (`x25519-dalek`) provides elliptic curve Diffie-Hellman operations for client identity keys and per-message ephemeral keys.
- **Key Derivation**: **HKDF-SHA256** (`hkdf` + `sha2`) derives high-entropy 256-bit symmetric keys from the Diffie-Hellman shared secret.
- **Authenticated Symmetric Cipher**: **ChaCha20-Poly1305 AEAD** (`chacha20poly1305`) encrypts message contents with a random 96-bit (12-byte) nonce per message, ensuring both confidentiality and tamper detection.
- **Wire Encoding**: Standard Base64 (`base64`) serializes the binary payload into ASCII strings compatible with telnet/TCP streams.

### Wire Payload & Forward Secrecy
Each encrypted whisper packs:
$$\text{Payload} = \text{EphemeralPublicKey}_{32\text{B}} \parallel \text{Nonce}_{12\text{B}} \parallel (\text{Ciphertext} \parallel \text{Poly1305Tag}_{16\text{B}})$$

Because the sender generates a fresh ephemeral X25519 keypair for every whisper:
1. **Forward Secrecy**: Even if a client's long-term identity key were compromised in the future, past whispered messages cannot be decrypted because ephemeral private keys are discarded immediately after message transmission.
2. **Tamper Resistance**: Any tampering or bit modification on intermediate nodes causes Poly1305 AEAD authentication to fail on the recipient client.

### Distributed Public Key Directory
- **Registration**: When a client logs in, it can publish its public key immediately (`<username> <base64_pubkey>`) or via `/pubkey <base64_pubkey>`.
- **Cluster Synchronization**: The server broadcasts `ClusterClientKey` and includes registered keys in `ClusterNewClient` and `ClusterSync`, replicating the public key directory across all nodes in the cluster.
- **Directory Query**: Any client can query the public key of a connected user via `/getkey <username>`.
- **Status Badges**: The `/users` command displays connected clients with `[e2ee]` capability badges (e.g. `Alice [e2ee], Bob [e2ee], Charlie`).

### Blind Cluster Routing
- When sending an encrypted message, the client executes `/etell <user> <base64_ciphertext>`.
- The central `server` actor wraps this in `ChatMessage::EncryptedTell` and forwards it via `ClusterSend { to, msg }` over the LZ4-compressed `system.network` inter-node link.
- The recipient node receives the opaque message and delivers `*E2EE* <sender>: <base64_ciphertext>` directly to the recipient's socket.
- Intermediate nodes inspect only the destination username `to` for routing; the message content remains encrypted at all times.

---

## 10. Dedicated Terminal Client (`distrib-chat-client`)

A dedicated async terminal client is provided under `src/bin/client.rs`.

### Client Features
- Automatically generates local X25519 identity keypairs upon launch (private key never leaves the client process).
- Automatically registers public key with the cluster upon connection.
- Performs transparent peer public key discovery (`/getkey <user>`) and maintains an in-memory peer key cache.
- Transparently encrypts outgoing whispers (`/tell <user> <message>`) and decrypts incoming whispers (`*E2EE* <sender>: <ciphertext>`), printing decrypted text in colored terminal output.
- Queues whispers while waiting for asynchronous peer public key lookups.

### Running the Dedicated Client

Open two separate terminal windows (with nodes running on ports 44441 and 44442):

**Terminal 1 (Alice on Node 1):**
```bash
cargo run --bin distrib-chat-client -- Alice 127.0.0.1:44441
```

**Terminal 2 (Bob on Node 2):**
```bash
cargo run --bin distrib-chat-client -- Bob 127.0.0.1:44442
```

### Interactive Client Commands
```text
/tell <user> <message>   - Send an End-to-End Encrypted whisper (transparent key exchange)
/plain <user> <message>  - Send an unencrypted whisper
/users                   - List connected users across the cluster and their E2EE capabilities
/getkey <user>           - Query and cache a user's E2EE public key
/keys                    - Display all locally cached peer public keys
/mykey                   - Display your local X25519 public key
/kick <user>             - Kick a user from the chat
/quit                    - Disconnect and exit
<message>                - Broadcast public message to all connected clients
```

### Full Interoperability with Telnet/Netcat
Standard unencrypted clients (`nc` or `telnet`) can continue to connect alongside E2EE clients:
- Standard clients participate in public chat broadcasts, `/users`, `/tell` (plain), `/kick`, etc.
- In `/users`, standard clients can see which users support E2EE.
- If an unencrypted client receives an E2EE whisper, it sees the opaque ciphertext `*E2EE* <sender>: <base64>`, confirming that non-E2EE parties cannot read private messages.
