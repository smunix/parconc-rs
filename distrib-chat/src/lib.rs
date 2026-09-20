//! Distributed Chat Server and E2EE Client library using elfo.

pub mod client;
pub mod crypto;
pub mod gossip;
pub mod protocol;
pub mod server;

use elfo::{msg, topology::Outcome};
use serde::Deserialize;

use crate::{
    client::{DeliverToClient, KickClient, PendingSockets},
    protocol::{
        ClusterBroadcast, ClusterClientDisconnected, ClusterClientKey, ClusterKick,
        ClusterNewClient, ClusterNodeLeft, ClusterPing, ClusterPong, ClusterSend, ClusterSync,
        GossipGetPeers, GossipPeersRoster,
    },
};

#[derive(Deserialize, Default)]
struct FileTomlConfig {
    #[serde(rename = "system.network", default)]
    network: Option<FileNetworkConfig>,
    #[serde(default)]
    discovery_gossip: Option<FileGossipConfig>,
}

#[derive(Deserialize, Default)]
struct FileNetworkConfig {
    #[serde(default)]
    listen: Vec<String>,
    #[serde(default)]
    discovery: Option<FileDiscoveryConfig>,
}

#[derive(Deserialize, Default)]
struct FileDiscoveryConfig {
    #[serde(default)]
    predefined: Vec<String>,
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct FileGossipConfig {
    pub node_name: Option<String>,
    pub listen_addr: Option<String>,
    pub is_seed: Option<bool>,
    #[serde(default)]
    pub seeds: Vec<String>,
}

/// Constructs the elfo topology for a distributed chat node.
pub fn build_topology(
    node_name: &str,
    config_path: &str,
    tcp_port: u16,
    pending_sockets: PendingSockets,
) -> elfo::Topology {
    let topology = elfo::Topology::empty();

    // Extract network & gossip discovery parameters from configuration file
    let toml_cfg: FileTomlConfig = std::fs::read_to_string(config_path)
        .ok()
        .and_then(|s| toml::from_str(&s).ok())
        .unwrap_or_default();

    let gossip_cfg = toml_cfg.discovery_gossip.unwrap_or_default();
    let network_cfg = toml_cfg.network.unwrap_or_default();

    let listen_addr = gossip_cfg
        .listen_addr
        .or_else(|| network_cfg.listen.first().cloned())
        .unwrap_or_else(|| "tcp://127.0.0.1:9301".to_string());

    let is_seed = gossip_cfg.is_seed.unwrap_or_else(|| {
        node_name.contains("us-east") || node_name.contains("eu") || node_name.contains("seed")
    });

    let initial_seeds = if !gossip_cfg.seeds.is_empty() {
        gossip_cfg.seeds
    } else {
        network_cfg
            .discovery
            .map(|d| d.predefined)
            .unwrap_or_default()
    };

    // System actor groups
    let loggers = topology.local("system.loggers");
    let configurers = topology.local("system.configurers").entrypoint();
    let network = topology.local("system.network");
    let network_addr = network.addr();

    // Dynamic Discovery & Gossip actor group (Approach A)
    let gossip_group = topology.local("discovery_gossip");
    let remote_gossip = topology.remote("discovery_gossip");

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

    // 5. Gossip routes internode gossip & heartbeats to remote gossip groups
    gossip_group.route_to(&remote_gossip, |envelope, _| {
        msg!(match envelope {
            GossipGetPeers { .. }
            | GossipPeersRoster { .. }
            | ClusterPing { .. }
            | ClusterPong { .. }
            | ClusterNodeLeft { .. } => Outcome::Broadcast,
            _ => Outcome::Discard,
        })
    });

    // 6. Gossip routes dynamic network reconfigurations to system.network
    gossip_group.route_to(&network, |envelope| {
        msg!(match envelope {
            elfo::messages::UpdateConfig { .. } => true,
            _ => false,
        })
    });

    // 7. Gossip routes node failure notices to the local server
    gossip_group.route_to(&server_group, |envelope| {
        msg!(match envelope {
            ClusterNodeLeft { .. } => true,
            _ => false,
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

    gossip_group.mount(gossip::blueprint(
        node_name.to_string(),
        listen_addr,
        initial_seeds,
        is_seed,
        network_addr,
    ));

    configurers.mount(elfo::batteries::configurer::from_path(
        &topology,
        config_path,
    ));

    topology
}
