//! Distributed Chat Server and E2EE Client library using elfo.

pub mod client;
pub mod crypto;
pub mod protocol;
pub mod server;

use elfo::{msg, topology::Outcome};

use crate::{
    client::{DeliverToClient, KickClient, PendingSockets},
    protocol::{
        ClusterBroadcast, ClusterClientDisconnected, ClusterClientKey, ClusterKick,
        ClusterNewClient, ClusterSend, ClusterSync,
    },
};

/// Constructs the elfo topology for a distributed chat node.
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
            | ClusterClientKey { .. }
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
