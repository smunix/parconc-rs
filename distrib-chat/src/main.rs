//! Distributed Chat Server in Rust using elfo.
//!
//! Based on the Distributed Chat problem described in Marlow's
//! "Parallel and Concurrent Programming in Haskell" (pages 262-270).

mod client;
mod protocol;
mod server;

use elfo::{msg, topology::Outcome};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    client::{DeliverToClient, KickClient, PendingSockets},
    protocol::{
        ClusterBroadcast, ClusterClientDisconnected, ClusterKick, ClusterNewClient, ClusterSend,
        ClusterSync,
    },
};

/// Constructs the elfo topology for a distributed chat node.
///
/// Each node in the cluster runs:
/// 1. `server`: Coordinates local clients and remote node peers.
/// 2. `clients`: Sharded local group managing individual connected TCP clients.
/// 3. `acceptor`: Local group listening on TCP for incoming client connections (telnet / nc).
/// 4. `server` (remote): Remote group proxy representing peer chat servers on other nodes.
/// 5. `system.network`: The `elfo-network` subsystem handling TCP clustering and message serialization.
/// 6. `system.configurers`: Reads TOML configuration (defining listen and discovery endpoints).
/// 7. `system.loggers`: Structured tracing and logging.
pub fn build_topology(
    config_path: &str,
    tcp_port: u16,
    pending_sockets: PendingSockets,
) -> elfo::Topology {
    let topology = elfo::Topology::empty();

    // System actor groups
    let loggers = topology.local("system.loggers");
    let configurers = topology.local("system.configurers").entrypoint();
    let network = topology.local("system.network");

    // Local chat server group
    let server_group = topology.local("server");

    // Sharded local client group
    let clients_group = topology.local("clients");

    // Local client TCP acceptor group
    let acceptor_group = topology.local("acceptor");

    // Remote peer group representing the "server" group on other cluster nodes
    let remote_server = topology.remote("server");

    // -------------------------------------------------------------------------
    // Routing definitions
    // -------------------------------------------------------------------------

    // 1. Acceptor routes new incoming connection events to the sharded clients group
    acceptor_group.route_all_to(&clients_group);

    // 2. Clients route chat commands/requests to the local central server
    clients_group.route_all_to(&server_group);

    // 3. Local server routes delivery notifications back to specific local clients
    server_group.route_to(&clients_group, |envelope| {
        msg!(match envelope {
            DeliverToClient { .. } | KickClient { .. } => true,
            _ => false,
        })
    });

    // 4. Local server routes cluster events across the network to peer nodes' servers
    server_group.route_to(&remote_server, |envelope, _| {
        msg!(match envelope {
            ClusterNewClient { .. }
            | ClusterClientDisconnected { .. }
            | ClusterBroadcast { .. }
            | ClusterSend { .. }
            | ClusterKick { .. }
            | ClusterSync { .. } => Outcome::Broadcast,
            _ => Outcome::Discard,
        })
    });

    // -------------------------------------------------------------------------
    // Mount actor blueprints
    // -------------------------------------------------------------------------
    loggers.mount(elfo::batteries::logger::init());
    network.mount(elfo::batteries::network::new(&topology));
    server_group.mount(server::blueprint());
    clients_group.mount(client::blueprint(pending_sockets.clone()));
    acceptor_group.mount(client::acceptor_blueprint(tcp_port, pending_sockets));

    configurers.mount(elfo::batteries::configurer::from_path(
        &topology,
        config_path,
    ));

    topology
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let node_name = args.next().unwrap_or_else(|| "node1".to_string());
    let tcp_port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(44444);

    let config_path = format!("config/{node_name}.toml");

    println!("============================================================");
    println!(" Starting Distributed Chat Node: {node_name}");
    println!(" Config path: {config_path}");
    println!(" Chat Client TCP port: {tcp_port}");
    println!(" Connect with: nc 127.0.0.1 {tcp_port}  (or telnet)");
    println!("============================================================");

    let pending_sockets = Arc::new(Mutex::new(HashMap::new()));
    let topology = build_topology(&config_path, tcp_port, pending_sockets);

    // Start elfo runtime and run until terminated
    elfo::init::start(topology).await;

    Ok(())
}
