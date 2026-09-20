# Distributed Chat Server in Rust using Elfo

A high-performance, fault-tolerant distributed chat server in Rust implemented using the [**elfo**](https://github.com/elfo-rs/elfo) actor framework.

This project is an idiomatic Rust port of the **Distributed Chat Server** problem described in Simon Marlow's book [***Parallel and Concurrent Programming in Haskell***](https://www.oreilly.com/library/view/parallel-and-concurrent/9781449335939/) (Chapter 12: *Distributed Programming with Cloud Haskell*, pages 262–270), originally implemented in `chat.hs`.

---

## 1. Problem Overview

The distributed chat problem simulates an IRC/Slack-like chat network spanning multiple distributed server instances across a cluster.

### Core Requirements
1. **Multi-Node Clustering**: Chat servers run as autonomous cluster nodes (e.g. Node 1, Node 2, Node 3) communicating over TCP.
2. **Client Ingestion**: Users connect via standard telnet or netcat (`nc <host> <port>`) to *any* node in the cluster.
3. **Nickname Registration & Collision Handling**:
   - On connecting, the user is greeted with `What is your name?`.
   - If the chosen name is already taken by a client on *any* node in the cluster, registration is rejected (`The name '<name>' is in use, please choose another.`), prompting again.
4. **Global Chat Broadcasts**:
   - Any message typed by a user on Node 1 is immediately relayed to all local clients on Node 1 *and* all remote clients on Node 2, Node 3, etc.
   - Format: `<Alice>: Hello world!`.
5. **Private Whispers (`/tell <user> <message>`)**:
   - Directed messaging routed specifically to the target client regardless of which node they are connected to.
   - If the target does not exist, the sender is informed (`<user> is not connected.`).
   - Format: `*Alice*: psst secret`.
6. **Administrative Kicks (`/kick <user>`)**:
   - Any user can kick another user anywhere in the cluster.
   - The victim is sent `You have been kicked: kicked by <kicker>` and their TCP connection is closed.
   - Kicker receives confirmation (`*** you kicked <victim>`).
   - Everyone receives a notice (`*** <victim> has disconnected`).
7. **Clean Disconnections (`/quit` or EOF)**:
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
| **Mailbox / Delivery** | `TChan Message` per local client | Dedicated actor mailbox per client actor; messages routed via `DeliverToClient` |
| **Internode Messaging** | Cloud Haskell `send pid (MsgSend ...)` | `elfo-network` TCP transport; `server` actor routes to `topology.remote("server")` |
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
        C1 -->|"Register / Broadcast / Tell / Kick"| S1["server actor<br/>(Central Node Coordinator)"]
        S1 -->|"DeliverToClient / KickClient"| C1
        S1 <==>|"ClusterBroadcast / ClusterSend / ClusterKick"| Net1["system.network<br/>(elfo-network)"]
    end

    subgraph Node2 ["Node 2 (e.g. port 44442)"]
        Net2["system.network<br/>(elfo-network)"] <==>|"ClusterBroadcast / ClusterSend / ClusterKick"| S2["server actor<br/>(Central Node Coordinator)"]
        S2 -->|"DeliverToClient / KickClient"| C2["clients group<br/>(sharded by ClientId)"]
        A2["acceptor actor<br/>(TCP 44442)"] -->|"NewClientConnection"| C2
        Telnet2["Telnet / nc (Bob)"] <==>|"TCP Stream"| C2
    end

    Net1 <====="TCP Cluster Mesh (9301 <-> 9302)"=====> Net2
```

### Actor Groups Explained

1. **`acceptor` (Local Group)**:
   - Binds the TCP listener for chat clients (e.g., `0.0.0.0:44441`).
   - For every incoming TCP connection, assigns a unique `ClientId` and sends `NewClientConnection { client_id }` to the `clients` group.
2. **`clients` (Local Sharded Group)**:
   - Routed by `MapRouter` on `ClientId`. When `NewClientConnection` arrives for an unknown `ClientId`, Elfo automatically spawns a dedicated client actor.
   - Attaches an async `Stream::generate` reader to asynchronously read newline-delimited commands from the TCP socket.
   - Manages client authentication (`readName` protocol), command parsing (`/tell`, `/kick`, `/quit`), and terminal writing.
3. **`server` (Local Group)**:
   - Central node directory. Tracks whether each known nickname is `Local(ClientId)` or `Remote`.
   - Dispatches local deliveries, rejects duplicate names, and relays cluster events across `system.network`.
   - Periodically reconciles cluster state via `ClusterSync` ticks.
4. **`system.network` (Elfo Battery)**:
   - Handles TCP connection establishment, heartbeats (`Ping`/`Pong`), reconnect backoff, and transparent message serialization between cluster nodes.
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

## 5. Configuration

Each node has a TOML configuration file specifying its network bind address and peer discovery endpoints:

### `config/node1.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9301"]
discovery.predefined = ["tcp://127.0.0.1:9302"]
```

### `config/node2.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9302"]
discovery.predefined = ["tcp://127.0.0.1:9301"]
```

### `config/node3.toml`
```toml
[system.network]
listen = ["tcp://127.0.0.1:9303"]
discovery.predefined = ["tcp://127.0.0.1:9301", "tcp://127.0.0.1:9302"]
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

You will see logs indicating that `system.network` has discovered peers and established data channels:
```
INFO system.network/server:33894:server - connection picked up ...
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
Available commands: /tell <user> <msg>, /kick <user>, /quit
*** Alice has connected
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
Available commands: /tell <user> <msg>, /kick <user>, /quit
*** Bob has connected
```

**Back on Alice's terminal (Node 1)**, Alice instantly receives:
```text
*** Bob has connected
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
python3 -c "
import socket, time

s1 = socket.create_connection(('127.0.0.1', 44441))
s1.recv(1024)
s1.sendall(b'Alice\n')
time.sleep(0.2); s1.recv(1024)

s2 = socket.create_connection(('127.0.0.1', 44442))
s2.recv(1024)
s2.sendall(b'Bob\n')
time.sleep(0.2); s2.recv(1024)

# Bob broadcasts
s2.sendall(b'Hello from Node 2!\n')
time.sleep(0.2)
print('Alice received broadcast:', s1.recv(1024).decode())

# Alice whispers to Bob
s1.sendall(b'/tell Bob secret\n')
time.sleep(0.2)
print('Bob received whisper:', s2.recv(1024).decode())

# Alice kicks Bob
s1.sendall(b'/kick Bob\n')
time.sleep(0.2)
print('Bob received kick:', s2.recv(1024).decode())
print('Alice received kick ack:', s1.recv(1024).decode())

s1.close()
s2.close()
print('All distributed chat tests passed successfully!')
"
```
