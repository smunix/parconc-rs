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

Each node runs an identical actor topology defined in [`src/lib.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/lib.rs) and instantiated in [`src/main.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/main.rs):

```mermaid
flowchart TD
    subgraph Node1 ["Node 1 (TCP Port 44441)"]
        Acceptor1["acceptor actor<br/>(TCP Listener 0.0.0.0:44441)"]
        Clients1["clients group<br/>(Sharded by ClientId via MapRouter)"]
        Server1["server actor<br/>(Central Node Coordinator)"]
        Network1["system.network<br/>(elfo-network with LZ4)"]
        Config1["system.configurers<br/>(Entrypoint: config/node1.toml)"]

        Acceptor1 -->|"NewClientConnection { client_id }"| Clients1
        Clients1 -->|"Register / Broadcast / Tell / Kick / ListUsers"| Server1
        Server1 -->|"DeliverToClient / KickClient"| Clients1
        Server1 <-->|"ClusterBroadcast / ClusterSend / ClusterKick / ClusterSync"| Network1
        Config1 -.->|"Config updates"| Acceptor1
        Config1 -.->|"Config updates"| Server1
        Config1 -.->|"Config updates"| Network1
    end

    subgraph Node2 ["Node 2 (TCP Port 44442)"]
        Network2["system.network<br/>(elfo-network with LZ4)"]
        Server2["server actor<br/>(Central Node Coordinator)"]
        Clients2["clients group<br/>(Sharded by ClientId via MapRouter)"]
        Acceptor2["acceptor actor<br/>(TCP Listener 0.0.0.0:44442)"]
        Config2["system.configurers<br/>(Entrypoint: config/node2.toml)"]

        Network2 <-->|"ClusterBroadcast / ClusterSend / ClusterKick / ClusterSync"| Server2
        Server2 -->|"DeliverToClient / KickClient"| Clients2
        Clients2 -->|"Register / Broadcast / Tell / Kick / ListUsers"| Server2
        Acceptor2 -->|"NewClientConnection { client_id }"| Clients2
        Config2 -.->|"Config updates"| Acceptor2
        Config2 -.->|"Config updates"| Server2
        Config2 -.->|"Config updates"| Network2
    end

    Alice["Alice (Telnet / Ratatui TUI)"] <-->|"TCP Stream"| Clients1
    Bob["Bob (Telnet / Ratatui TUI)"] <-->|"TCP Stream"| Clients2

    Network1 <-->|"TCP Mesh with LZ4 (127.0.0.1:9301 &harr; 9302)"| Network2
```

### Actor Groups Explained

1. **`acceptor` (Local Group, Singleton)**:
   - Binds the TCP listener for incoming chat clients (e.g. `0.0.0.0:44441`).
   - For every incoming connection, increments atomic `NEXT_CLIENT_ID`, stashes the `tokio::net::TcpStream` in `pending_sockets`, and fires `NewClientConnection { client_id }` toward the `clients` group.
2. **`clients` (Local Sharded Group)**:
   - Configured with `elfo::routers::MapRouter` on `ClientId`. When `NewClientConnection` arrives for an unknown ID, Elfo dynamically instantiates a dedicated client actor.
   - Extracts its `TcpStream` from `pending_sockets`, splits it into read and write halves, and attaches an asynchronous `Stream::generate` reader.
   - Handles the login handshake (`What is your name?`), command parsing (`/users`, `/tell`, `/etell`, `/pubkey`, `/getkey`, `/kick`, `/quit`), and TCP socket writing.
3. **`server` (Local Group, Singleton)**:
   - Central node directory. Tracks whether each known nickname is `Local(ClientId)` or `Remote`, along with optional E2EE public keys.
   - Responds to client registration requests, verifies nickname uniqueness across the cluster, and handles `/users` directory queries.
   - Routes whispers, kicks, and broadcasts locally and across the Elfo network mesh.
   - Hosts a periodic timer tick (`SyncTick`, every 3s) broadcasting `ClusterSync` for state convergence.
4. **`system.network` (Elfo Battery)**:
   - Establishes and monitors inter-node TCP connections using predefined discovery targets.
   - Transparently multiplexes, serializes, and deserializes Elfo messages sent to `topology.remote("server")`.
   - Negotiates and applies LZ4 frame compression (`compression.lz4 = "Preferred"`).
5. **`system.configurers` (Elfo Battery Entrypoint)**:
   - Dynamically loads and validates node configuration from TOML files, distributing config sections to groups at startup.

---

## 4. Protocol Interaction & Sequence Diagrams

### 4.1 Client Connection, Handshake & Nickname Registration
When a client connects over TCP, the `acceptor` registers the socket and hands off control to a dedicated `clients` actor instance. The client negotiates their nickname and optionally publishes their X25519 public key:

```mermaid
sequenceDiagram
    autonumber
    actor Alice as Alice (TCP Client)
    participant Acceptor as acceptor
    participant Clients as clients (ClientId: 1)
    participant Server1 as server (Node 1)
    participant Net1 as system.network (Node 1)
    participant Net2 as system.network (Node 2)
    participant Server2 as server (Node 2)
    participant Bob as clients (Bob on Node 2)

    Alice->>Acceptor: TCP Connect (SYN)
    Acceptor->>Acceptor: client_id = NEXT_CLIENT_ID.fetch_add(1)
    Acceptor->>Acceptor: pending_sockets.insert(1, tcp_stream)
    Acceptor->>Clients: NewClientConnection { client_id: 1 }
    Clients->>Clients: Elfo spawns actor for ClientId=1
    Clients->>Clients: pending_sockets.remove(1) & split socket
    Clients-->>Alice: "What is your name?\r\n"
    Alice->>Clients: "Alice <base64_pubkey>\r\n"
    Clients->>Server1: RegisterClient { client_id: 1, name: "Alice", pubkey: Some(...) }
    
    alt Nickname Already Taken
        Server1-->>Clients: Err("The name 'Alice' is in use...")
        Clients-->>Alice: "The name 'Alice' is in use, please choose another.\r\nWhat is your name?\r\n"
    else Nickname Available
        Server1->>Server1: clients.insert("Alice", Local(1), pubkey)
        Server1-->>Clients: Ok(())
        Server1->>Net1: ClusterNewClient { name: "Alice", pubkey: Some(...) }
        Net1->>Net2: TCP Mesh (LZ4 Frame): ClusterNewClient
        Net2->>Server2: ClusterNewClient { name: "Alice", pubkey: Some(...) }
        Server2->>Server2: clients.insert("Alice", Remote, pubkey)
        Server2->>Bob: DeliverToClient { msg: Notice("*** Alice has connected") }
        Clients-->>Alice: "Welcome to the distributed chat, Alice!\r\nAvailable commands: ...\r\n"
    end
```

---

### 4.2 Global Chat Broadcast Flow
Public chat messages typed by any client are fanned out to both local clients on the same node and all remote clients across the cluster mesh:

```mermaid
sequenceDiagram
    autonumber
    actor Alice as Alice (Node 1)
    participant C_Alice as clients (Alice)
    participant S1 as server (Node 1)
    participant C_Charlie as clients (Charlie, Node 1)
    participant Net1 as system.network (Node 1)
    participant Net2 as system.network (Node 2)
    participant S2 as server (Node 2)
    participant C_Bob as clients (Bob, Node 2)

    Alice->>C_Alice: "Hello cluster!\r\n"
    C_Alice->>S1: BroadcastRequest { from: "Alice", msg: "Hello cluster!" }
    
    par Local Node Delivery
        S1->>C_Alice: DeliverToClient { msg: Broadcast("<Alice>: Hello cluster!") }
        C_Alice-->>Alice: "<Alice>: Hello cluster!\r\n"
        S1->>C_Charlie: DeliverToClient { msg: Broadcast("<Alice>: Hello cluster!") }
        C_Charlie-->>Charlie: "<Alice>: Hello cluster!\r\n"
    and Remote Cluster Broadcast
        S1->>Net1: ClusterBroadcast { msg: Broadcast("<Alice>: Hello cluster!") }
        Net1->>Net2: TCP Mesh (LZ4 Compressed)
        Net2->>S2: ClusterBroadcast { msg: Broadcast("<Alice>: Hello cluster!") }
        S2->>C_Bob: DeliverToClient { msg: Broadcast("<Alice>: Hello cluster!") }
        C_Bob-->>Bob: "<Alice>: Hello cluster!\r\n"
    end
```

---

### 4.3 Direct Private Whisper (`/tell`) Flow Across Nodes
Private messages are addressed to a specific nickname. If the recipient is hosted on a remote cluster node, the whisper is routed via `ClusterSend`:

```mermaid
sequenceDiagram
    autonumber
    actor Alice as Alice (Node 1)
    participant C_Alice as clients (Alice, Node 1)
    participant S1 as server (Node 1)
    participant Net1 as system.network (Node 1)
    participant Net2 as system.network (Node 2)
    participant S2 as server (Node 2)
    participant C_Bob as clients (Bob, Node 2)
    actor Bob as Bob (Node 2)

    Alice->>C_Alice: "/tell Bob Meet me at noon\r\n"
    C_Alice->>S1: TellRequest { from: "Alice", to: "Bob", msg: "Meet me at noon" }
    
    S1->>S1: Lookup "Bob" -> ClientEntry::Remote
    
    par Local Echo to Sender
        S1->>C_Alice: DeliverToClient { msg: Tell { from: "Alice", msg: "Meet me at noon" } }
        C_Alice-->>Alice: "*Alice*: Meet me at noon\r\n"
    and Inter-Node Routed Dispatch
        S1->>Net1: ClusterSend { to: "Bob", msg: Tell { from: "Alice", msg: "Meet me at noon" } }
        Net1->>Net2: TCP Mesh (LZ4 Compressed)
        Net2->>S2: ClusterSend { to: "Bob", msg: Tell { from: "Alice", msg: "Meet me at noon" } }
        S2->>S2: Lookup "Bob" -> ClientEntry::Local(target_id)
        S2->>C_Bob: DeliverToClient { client_id: target_id, msg: Tell { from: "Alice", msg: "Meet me at noon" } }
        C_Bob-->>Bob: "*Alice*: Meet me at noon\r\n"
    end
    S1-->>C_Alice: Ok(())
```

---

### 4.4 End-to-End Encrypted (E2EE) Whisper Flow (`/etell`)
With E2EE enabled, clients perform client-side ephemeral Diffie-Hellman key agreement and symmetric AEAD encryption. Intermediate servers forward opaque Base64 ciphertexts blindly:

```mermaid
sequenceDiagram
    autonumber
    actor Alice as Alice (TUI Client)
    participant C_Alice as clients (Node 1)
    participant S1 as server (Node 1)
    participant Net1 as system.network (Node 1)
    participant Net2 as system.network (Node 2)
    participant S2 as server (Node 2)
    participant C_Bob as clients (Node 2)
    actor Bob as Bob (TUI Client)

    Note over Alice,Bob: Step 1: Key Discovery & Caching
    Alice->>C_Alice: "/getkey Bob\r\n"
    C_Alice->>S1: GetKeyRequest { target: "Bob" }
    S1-->>C_Alice: Ok("<Bob_Public_Key_Base64>")
    C_Alice-->>Alice: "*** KEY Bob <Bob_Public_Key_Base64>\r\n"
    Alice->>Alice: Cache Bob's X25519 Public Key in memory

    Note over Alice,Bob: Step 2: Client-Side Cryptographic Construction
    Alice->>Alice: Generate Ephemeral Secret k_eph & Public P_eph
    Alice->>Alice: Compute DH Secret: S = ECDH(k_eph, P_Bob)
    Alice->>Alice: Derive 32B Symmetric Key via HKDF-SHA256(S, "distrib-chat-e2ee-v1")
    Alice->>Alice: Encrypt plaintext with ChaCha20-Poly1305 + 12B random nonce
    Alice->>Alice: Pack: P_eph (32B) + Nonce (12B) + Ciphertext + Tag (16B) -> Base64

    Note over Alice,Bob: Step 3: Zero-Knowledge Blind Relaying
    Alice->>C_Alice: "/etell Bob <base64_payload>\r\n"
    C_Alice->>S1: EncryptedTellRequest { from: "Alice", to: "Bob", ciphertext: "<b64>" }
    S1->>Net1: ClusterSend { to: "Bob", msg: EncryptedTell { from: "Alice", ciphertext: "<b64>" } }
    Net1->>Net2: TCP Mesh (LZ4 Compressed, opaque payload)
    Net2->>S2: ClusterSend { to: "Bob", msg: EncryptedTell { from: "Alice", ciphertext: "<b64>" } }
    S2->>C_Bob: DeliverToClient { client_id: target_id, msg: EncryptedTell { from: "Alice", ciphertext: "<b64>" } }
    C_Bob-->>Bob: "*E2EE* Alice: <base64_payload>\r\n"

    Note over Alice,Bob: Step 4: Client-Side Decryption & Verification
    Bob->>Bob: Unpack Base64: Extract P_eph (32B), Nonce (12B), Ciphertext+Tag
    Bob->>Bob: Compute DH Secret: S = ECDH(k_Bob_static, P_eph)
    Bob->>Bob: Derive 32B Symmetric Key via HKDF-SHA256(S, "distrib-chat-e2ee-v1")
    Bob->>Bob: Decrypt & Verify MAC with ChaCha20-Poly1305
    Bob-->>Bob: Render in TUI: [E2EE] Alice: <plaintext> (Green Badge)
```

---

### 4.5 Administrative Remote Kick & Disconnection Flow
Any client can kick another user anywhere in the cluster. When the kick reaches the target node, the connection is closed and the unregistration propagates cluster-wide:

```mermaid
sequenceDiagram
    autonumber
    actor Alice as Alice (Moderator on Node 1)
    participant C_Alice as clients (Alice, Node 1)
    participant S1 as server (Node 1)
    participant Net1 as system.network (Node 1)
    participant Net2 as system.network (Node 2)
    participant S2 as server (Node 2)
    participant C_Bob as clients (Bob, Node 2)
    actor Bob as Bob (Victim on Node 2)

    Alice->>C_Alice: "/kick Bob\r\n"
    C_Alice->>S1: KickRequest { kicker: "Alice", victim: "Bob" }
    S1->>S1: Lookup "Bob" -> ClientEntry::Remote
    S1->>Net1: ClusterKick { victim: "Bob", by: "Alice" }
    Net1->>Net2: TCP Mesh (LZ4)
    Net2->>S2: ClusterKick { victim: "Bob", by: "Alice" }
    S2->>S2: Lookup "Bob" -> ClientEntry::Local(target_id)
    S2->>C_Bob: KickClient { client_id: target_id, reason: "kicked by Alice" }
    C_Bob-->>Bob: "You have been kicked: kicked by Alice\r\n"
    C_Bob->>C_Bob: Close TCP connection & break recv loop
    C_Bob->>S2: UnregisterClient { client_id: target_id, name: "Bob" }
    S2->>S2: clients.remove("Bob")
    S2->>Net2: ClusterClientDisconnected { name: "Bob" }
    Net2->>Net1: TCP Mesh (LZ4)
    Net1->>S1: ClusterClientDisconnected { name: "Bob" }
    S1->>S1: clients.remove("Bob")
    S1->>C_Alice: DeliverToClient { msg: Notice("*** Bob has disconnected") }
    C_Alice-->>Alice: "*** You kicked Bob\r\n*** Bob has disconnected\r\n"
```

## 5. Message Protocol

### Terminal / Local Client Messages ([`src/protocol.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/protocol.rs))
- `ChatMessage::Notice(String)`: System announcements (`*** Alice has connected`).
- `ChatMessage::Tell { from, msg }`: Whispers (`*Alice*: hello`).
- `ChatMessage::EncryptedTell { from, ciphertext }`: End-to-end encrypted whispers (`*E2EE* Alice: <base64>`).
- `ChatMessage::Broadcast { from, msg }`: Chat messages (`<Alice>: hello`).

### Cluster Synchronization Messages (Internode)
- `ClusterNewClient { name, pubkey }`: Broadcast when a user registers on any node.
- `ClusterClientKey { name, pubkey }`: Broadcast when a user registers or updates their E2EE public key.
- `ClusterClientDisconnected { name }`: Broadcast when a user disconnects or quits.
- `ClusterBroadcast { msg }`: Relays a public chat message across all cluster nodes.
- `ClusterSend { to, msg }`: Routes a private message (whisper or E2EE payload) across nodes to the destination client.
- `ClusterKick { victim, by }`: Directs the remote node hosting `victim` to terminate that client's connection.
- `ClusterSync { clients }`: Periodic cluster reconciliation ensuring convergence even after network hiccups.

---

## 6. Configuration & Compression

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

## 7. How to Build & Run

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

## 8. Interactive Walkthrough

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

## 9. Automated Integration Tests

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

## 10. End-to-End Encryption (E2EE) Cryptographic Architecture & Flow

The distributed chat system features full **End-to-End Encryption (E2EE)** for private whispers between users across the cluster. Intermediate server nodes act purely as blind routing relays: they forward opaque Base64 ciphertext payloads and have **zero visibility** into the plaintext contents, nor do they possess private keys.

### Cryptographic Flow & Architecture

```mermaid
flowchart TD
    subgraph Sender ["Sender (Alice's Client)"]
        SK_A["Ephemeral Secret<br/>(k_eph: 32B, fresh CSPRNG)"] --> DH_A["Diffie-Hellman<br/>ECDH(k_eph, P_Bob)"]
        PK_B["Bob's Static Public Key<br/>(P_Bob: 32B from /getkey)"] --> DH_A
        DH_A --> Secret_A["Shared Secret (32B)"]
        Secret_A --> HKDF_A["HKDF-SHA256<br/>info = 'distrib-chat-e2ee-v1'"]
        HKDF_A --> SymKey_A["Symmetric Key (32B)"]
        Nonce_A["Random Nonce (12B)"] --> AEAD_Enc["ChaCha20-Poly1305<br/>Encrypt & Sign"]
        SymKey_A --> AEAD_Enc
        Plaintext["Plaintext Message"] --> AEAD_Enc
        AEAD_Enc --> CT_Tag["Ciphertext + Poly1305 Tag (16B)"]
        
        PK_Eph["Ephemeral Public Key<br/>(P_eph: 32B)"] --> Packer["Binary Payload Packer"]
        Nonce_A --> Packer
        CT_Tag --> Packer
        Packer --> B64_Enc["Base64 Encode"]
        B64_Enc --> WireMsg["Wire Format:<br/>/etell Bob &lt;base64_payload&gt;"]
    end

    subgraph Relays ["Blind Cluster Relays (Zero-Knowledge)"]
        WireMsg --> Node1_Relay["Node 1 (server)<br/>Inspects only 'Bob' routing key"]
        Node1_Relay -->|"ClusterSend { to: 'Bob', msg }<br/>(TCP + LZ4 Mesh)"| Node2_Relay["Node 2 (server)<br/>Forwards to Bob's socket"]
        Node2_Relay --> WireDelivery["*E2EE* Alice: &lt;base64_payload&gt;"]
    end

    subgraph Recipient ["Recipient (Bob's Client)"]
        WireDelivery --> B64_Dec["Base64 Decode"]
        B64_Dec --> Unpacker["Binary Payload Unpacker"]
        Unpacker --> Ext_PK_Eph["Extracted P_eph (32B)"]
        Unpacker --> Ext_Nonce["Extracted Nonce (12B)"]
        Unpacker --> Ext_CT["Extracted Ciphertext + Tag"]
        
        Ext_PK_Eph --> DH_B["Diffie-Hellman<br/>ECDH(k_Bob_static, P_eph)"]
        SK_B["Bob's Static Secret<br/>(k_Bob_static: 32B in memory)"] --> DH_B
        DH_B --> Secret_B["Identical Shared Secret (32B)"]
        Secret_B --> HKDF_B["HKDF-SHA256<br/>info = 'distrib-chat-e2ee-v1'"]
        HKDF_B --> SymKey_B["Identical Symmetric Key (32B)"]
        
        SymKey_B --> AEAD_Dec["ChaCha20-Poly1305<br/>Verify MAC & Decrypt"]
        Ext_Nonce --> AEAD_Dec
        Ext_CT --> AEAD_Dec
        AEAD_Dec --> DecryptedPlaintext["Decrypted Plaintext<br/>Rendered in TUI with [E2EE] badge"]
    end
```

### Cryptographic Stack & Primitives
- **Key Agreement (Diffie-Hellman)**: **X25519** (`x25519-dalek`) provides elliptic curve Diffie-Hellman operations for client identity keys and per-message ephemeral keys.
- **Key Derivation**: **HKDF-SHA256** (`hkdf` + `sha2`) derives high-entropy 256-bit symmetric keys from the Diffie-Hellman shared secret.
- **Authenticated Symmetric Cipher**: **ChaCha20-Poly1305 AEAD** (`chacha20poly1305`) encrypts message contents with a random 96-bit (12-byte) nonce per message, ensuring both confidentiality and tamper detection.
- **Wire Encoding**: Standard Base64 (`base64`) serializes the binary payload into ASCII strings compatible with telnet/TCP streams.

### Binary Wire Payload & Forward Secrecy

Before Base64 serialization, binary bytes are packed contiguously into a single envelope:

```text
+------------------------------------+------------------+-----------------------------+--------------------------+
| Ephemeral Public Key (32 bytes)    | Nonce (12 bytes) | ChaCha20 Ciphertext (var)   | Poly1305 Tag (16 bytes)  |
+------------------------------------+------------------+-----------------------------+--------------------------+
| 0                               31 | 32            43 | 44             (len - 17)   | (len - 16)      (len - 1)|
```

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

## 11. Modern Ratatui Terminal User Interface (`distrib-chat-client`)

A full-fledged, modern Terminal User Interface (TUI) client built with **Ratatui** and **Crossterm** is provided under [`src/bin/client.rs`](file:///home/smunix/Projects/scratchpad/rs/parconc-examples/distrib-chat/src/bin/client.rs).

### TUI Architecture & Layout
The interface utilizes a responsive multi-pane layout:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  DISTRIBUTED CHAT    [End-to-End Encrypted (E2EE) Actor Mesh]                          │
│  User: Alice  |  Server: 127.0.0.1:44441  |  X25519 Key: 8Zf...  |  Status: ● Connected│
├─────────────────────────────────────────────────────────┬──────────────────────────────┤
│ Messages (14) [Auto-Scroll]                             │ Online Users (2)             │
│ [09:32:01] *** Alice has connected                      │ ● Alice [E2EE] (you)         │
│ [09:32:05] *** Bob has connected                        │ ● Bob [E2EE]                 │
│ [09:32:12] <Alice>: Hello cluster!                      ├──────────────────────────────┤
│ [09:32:15] <Bob>: Hey Alice!                            │ Key Directory                │
│ [09:32:20] [E2EE] Bob: Top secret whisper via ChaCha20  │ Cached Keys: 1               │
│ [09:32:25] [E2EE -> Bob]: Encrypted reply received!     │ ✔ Bob                        │
├─────────────────────────────────────────────────────────┴──────────────────────────────┤
│ Message / Command                                                                      │
│ > /tell Bob Secret whisper                                                             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ [Enter] Send | [/tell <user> <msg>] E2EE Whisper | [/users] Refresh | [PgUp/PgDn] Scroll | [Esc] Quit
└────────────────────────────────────────────────────────────────────────────────────────┘
```

- **Header Bar**: Displays current user, server address, local X25519 public key fingerprint, and cluster connection status.
- **Messages Pane (Left, 75%)**:
  - Color-coded chat history with UTC timestamps `[HH:MM:SS]`.
  - `[E2EE]` badges for private whispers (incoming in green, outgoing in magenta).
  - Cyan headers for public broadcasts `<User>`.
  - Yellow italic text for cluster notices and Red bold text for alerts/errors.
  - Smooth scrolling support (Auto-scroll to latest message by default, manual scrolling via `PgUp`/`PgDn` or `Up`/`Down`).
- **Sidebar (Right, 25%)**:
  - **Online Users**: Live list of active cluster participants, distinguishing E2EE-capable users (`● User [E2EE]`) from unencrypted clients (`○ User [plain]`). Automatically refreshed on join/leave events.
  - **Key Directory**: Shows cached peer public keys and verified cryptographic contacts.
- **Input Box**: Active text input with cursor support, command parsing, and live typing.
- **Footer**: Keybinding hints and navigation reference.

### Client Features
- **Zero-Knowledge Privacy**: Automatically generates local X25519 identity keypairs upon launch (private key never leaves process memory).
- **Automatic Key Discovery**: Automatically registers public key with the cluster upon connection, queries peer public keys on demand (`/getkey <user>`), and caches them locally.
- **Transparent Cryptography**: Automatically encrypts outgoing whispers (`/tell <user> <message>`) and decrypts incoming whispers (`*E2EE* <sender>: <ciphertext>`).
- **Whisper Queueing**: Seamlessly queues outgoing whispers while awaiting asynchronous peer public key resolution from the server.
- **Terminal Safety**: Installs an emergency panic hook and clean exit handlers ensuring terminal raw mode and alternate screen are always restored.

### Running the Dedicated Client

Start two cluster nodes:
```bash
# Terminal 1: Node 1
cargo run --bin distrib-chat -- node1 44441

# Terminal 2: Node 2
cargo run --bin distrib-chat -- node2 44442
```

Launch the Ratatui TUI clients:
```bash
# Terminal 3: Alice on Node 1
cargo run --bin distrib-chat-client -- Alice 127.0.0.1:44441

# Terminal 4: Bob on Node 2
cargo run --bin distrib-chat-client -- Bob 127.0.0.1:44442
```

### Interactive Client Commands & Keybindings
```text
/tell <user> <message>   - Send an End-to-End Encrypted whisper (transparent key exchange)
/plain <user> <message>  - Send an unencrypted whisper
/users                   - Refresh connected users across the cluster
/getkey <user>           - Query and cache a user's E2EE public key
/keys                    - Display all locally cached peer public keys
/mykey                   - Display your local X25519 public key
/kick <user>             - Kick a user from the chat
/quit                    - Disconnect and exit
<message>                - Broadcast public message to all connected clients

Keybindings:
  Enter                  - Send current message or execute command
  PgUp / Up              - Scroll message history upward (manual mode)
  PgDn / Down            - Scroll message history downward (resumes auto-scroll at bottom)
  Esc / Ctrl+C           - Disconnect and quit
```

### Full Interoperability with Telnet/Netcat
Standard unencrypted clients (`nc` or `telnet`) can continue to connect alongside E2EE clients:
- Standard clients participate in public chat broadcasts, `/users`, `/tell` (plain), `/kick`, etc.
- In `/users`, standard clients can see which users support E2EE.
- If an unencrypted client receives an E2EE whisper, it sees the opaque ciphertext `*E2EE* <sender>: <base64>`, confirming that non-E2EE parties cannot read private messages.
