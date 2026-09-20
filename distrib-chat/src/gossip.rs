//! Dynamic Gossip-Based Seed Peering Discovery actor group.
//!
//! Implements decentralized dynamic cluster topology discovery (Approach A):
//! - Static Seed Nodes: Long-lived regional nodes act as stable rendezvous points (e.g. us-east, eu).
//! - Non-seed nodes boot with only seed addresses configured in their initial predefined list.
//! - Dynamic Peer Discovery: Nodes exchange their known cluster membership roster (`GossipGetPeers` / `GossipPeersRoster`).
//! - Dynamic Topology Reconfiguration: When new peers are discovered, the actor dynamically builds
//!   an updated `system.network` configuration and sends `UpdateConfig` to `system.network`.
//! - `elfo_network` diffs the predefined list and immediately dials TCP connections to the newly discovered peers.
//! - Heartbeat & Liveness: Nodes periodically ping peers (`ClusterPing` / `ClusterPong`).
//!   If a peer fails to respond within the heartbeat window, it is pruned and announced via `ClusterNodeLeft`.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use elfo::{addr::Addr, messages::UpdateConfig, msg, prelude::*, time::Interval};
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::protocol::{
    ClusterNodeLeft, ClusterPing, ClusterPong, GossipGetPeers, GossipPeersRoster, PeerEntry,
};

#[message]
struct GossipTick;

#[message]
struct HeartbeatTick;

#[message]
struct FailureCheckTick;

/// Liveness state for a discovered cluster peer.
#[derive(Debug, Clone)]
pub struct PeerState {
    pub node_name: String,
    pub listen_addr: String,
    pub is_seed: bool,
    pub last_seen: Instant,
    pub is_alive: bool,
    pub missed_heartbeats: u32,
}

/// Creates a blueprint for the dynamic gossip discovery actor group.
pub fn blueprint(
    node_name: String,
    listen_addr: String,
    initial_seeds: Vec<String>,
    is_seed: bool,
    network_addr: Addr,
) -> Blueprint {
    ActorGroup::new().exec(move |ctx| {
        let node_name = node_name.clone();
        let listen_addr = listen_addr.clone();
        let initial_seeds = initial_seeds.clone();
        DiscoveryGossip::new(
            ctx,
            node_name,
            listen_addr,
            initial_seeds,
            is_seed,
            network_addr,
        )
        .main()
    })
}

struct DiscoveryGossip {
    ctx: Context,
    node_name: String,
    listen_addr: String,
    is_seed: bool,
    network_addr: Addr,
    peers: HashMap<String, PeerState>,
    active_transports: HashSet<String>,
    sequence: u64,
    heartbeat_timeout: Duration,
}

impl DiscoveryGossip {
    fn new(
        ctx: Context,
        node_name: String,
        listen_addr: String,
        initial_seeds: Vec<String>,
        is_seed: bool,
        network_addr: Addr,
    ) -> Self {
        let mut active_transports = HashSet::new();
        for seed in &initial_seeds {
            if seed != &listen_addr {
                active_transports.insert(seed.clone());
            }
        }

        Self {
            ctx,
            node_name,
            listen_addr,
            is_seed,
            network_addr,
            peers: HashMap::new(),
            active_transports,
            sequence: 0,
            heartbeat_timeout: Duration::from_secs(6),
        }
    }

    async fn main(mut self) {
        info!(
            node = %self.node_name,
            listen = %self.listen_addr,
            is_seed = self.is_seed,
            initial_seeds = ?self.active_transports,
            "Discovery gossip actor started"
        );

        // Gossip query interval: 800ms
        self.ctx
            .attach(Interval::new(GossipTick))
            .start(Duration::from_millis(800));

        // Heartbeat ping interval: 2.0s
        self.ctx
            .attach(Interval::new(HeartbeatTick))
            .start(Duration::from_millis(2000));

        // Failure detection tick: 2.0s
        self.ctx
            .attach(Interval::new(FailureCheckTick))
            .start(Duration::from_millis(2000));

        // Initial burst: request peers from connected seeds immediately
        self.broadcast_get_peers().await;

        while let Some(envelope) = self.ctx.recv().await {
            msg!(match envelope {
                GossipTick => {
                    self.broadcast_get_peers().await;
                }

                HeartbeatTick => {
                    self.broadcast_heartbeats().await;
                }

                FailureCheckTick => {
                    self.check_failures().await;
                }

                GossipGetPeers {
                    sender_node,
                    sender_listen_addr,
                    is_seed,
                } => {
                    self.handle_get_peers(sender_node, sender_listen_addr, is_seed)
                        .await;
                }

                GossipPeersRoster { sender_node, peers } => {
                    self.handle_peers_roster(sender_node, peers).await;
                }

                ClusterPing {
                    sender_node,
                    sequence,
                } => {
                    self.handle_ping(sender_node, sequence).await;
                }

                ClusterPong {
                    sender_node,
                    sequence,
                } => {
                    self.handle_pong(sender_node, sequence);
                }

                ClusterNodeLeft { node_name, reason } => {
                    self.handle_node_left(node_name, reason);
                }
            });
        }
    }

    /// Broadcasts a request for peer lists to all currently connected nodes.
    async fn broadcast_get_peers(&self) {
        let _ = self
            .ctx
            .send(GossipGetPeers {
                sender_node: self.node_name.clone(),
                sender_listen_addr: self.listen_addr.clone(),
                is_seed: self.is_seed,
            })
            .await;
    }

    /// Broadcasts periodic heartbeat pings to verify peer liveness.
    async fn broadcast_heartbeats(&mut self) {
        self.sequence += 1;
        let _ = self
            .ctx
            .send(ClusterPing {
                sender_node: self.node_name.clone(),
                sequence: self.sequence,
            })
            .await;
    }

    /// Checks if any known peers have exceeded the heartbeat timeout.
    async fn check_failures(&mut self) {
        let now = Instant::now();
        let mut timed_out_nodes = Vec::new();

        for (name, state) in self.peers.iter_mut() {
            if state.is_alive && now.duration_since(state.last_seen) > self.heartbeat_timeout {
                state.is_alive = false;
                state.missed_heartbeats += 1;
                timed_out_nodes.push((name.clone(), state.listen_addr.clone()));
            }
        }

        for (node_name, listen_addr) in timed_out_nodes {
            warn!(
                node = %self.node_name,
                peer = %node_name,
                addr = %listen_addr,
                timeout = ?self.heartbeat_timeout,
                "Heartbeat timeout detected; peer marked disconnected"
            );

            let event = ClusterNodeLeft {
                node_name: node_name.clone(),
                reason: format!("Heartbeat timeout (> {:?})", self.heartbeat_timeout),
            };

            // Broadcast to other peers and notify local server
            let _ = self.ctx.send(event).await;
        }
    }

    /// Handles an incoming query for peers.
    async fn handle_get_peers(
        &mut self,
        sender_node: String,
        sender_listen_addr: String,
        is_seed: bool,
    ) {
        if sender_node == self.node_name {
            return;
        }

        let mut needs_reconfigure = false;

        // Record or refresh the sender in our peer table
        let entry = self
            .peers
            .entry(sender_node.clone())
            .or_insert_with(|| PeerState {
                node_name: sender_node.clone(),
                listen_addr: sender_listen_addr.clone(),
                is_seed,
                last_seen: Instant::now(),
                is_alive: true,
                missed_heartbeats: 0,
            });

        entry.last_seen = Instant::now();
        entry.is_alive = true;
        entry.missed_heartbeats = 0;

        // If the sender's listen address is not in our active transport list, add it
        if sender_listen_addr != self.listen_addr
            && !self.active_transports.contains(&sender_listen_addr)
        {
            info!(
                node = %self.node_name,
                peer = %sender_node,
                addr = %sender_listen_addr,
                "Discovered new peer from GossipGetPeers; adding to active transports"
            );
            self.active_transports.insert(sender_listen_addr.clone());
            needs_reconfigure = true;
        }

        // Build current active peer roster
        let mut roster = Vec::new();
        roster.push(PeerEntry {
            node_name: self.node_name.clone(),
            listen_addr: self.listen_addr.clone(),
            is_seed: self.is_seed,
        });

        for peer in self.peers.values() {
            if peer.is_alive {
                roster.push(PeerEntry {
                    node_name: peer.node_name.clone(),
                    listen_addr: peer.listen_addr.clone(),
                    is_seed: peer.is_seed,
                });
            }
        }

        // Reply with roster
        let _ = self
            .ctx
            .send(GossipPeersRoster {
                sender_node: self.node_name.clone(),
                peers: roster,
            })
            .await;

        if needs_reconfigure {
            self.reconfigure_network().await;
        }
    }

    /// Handles an incoming peer roster from a gossip partner.
    async fn handle_peers_roster(&mut self, sender_node: String, peers: Vec<PeerEntry>) {
        if sender_node == self.node_name {
            return;
        }

        let mut changed = false;

        for peer in peers {
            if peer.node_name == self.node_name || peer.listen_addr == self.listen_addr {
                continue;
            }

            let entry = self.peers.entry(peer.node_name.clone()).or_insert_with(|| {
                info!(
                    node = %self.node_name,
                    discovered_peer = %peer.node_name,
                    addr = %peer.listen_addr,
                    is_seed = peer.is_seed,
                    "Discovered new cluster peer via gossip roster"
                );
                PeerState {
                    node_name: peer.node_name.clone(),
                    listen_addr: peer.listen_addr.clone(),
                    is_seed: peer.is_seed,
                    last_seen: Instant::now(),
                    is_alive: true,
                    missed_heartbeats: 0,
                }
            });

            entry.last_seen = Instant::now();
            entry.is_alive = true;

            if !self.active_transports.contains(&peer.listen_addr) {
                self.active_transports.insert(peer.listen_addr.clone());
                changed = true;
            }
        }

        if changed {
            info!(
                node = %self.node_name,
                total_transports = self.active_transports.len(),
                "New peer transports detected; dynamically reconfiguring Elfo cluster mesh"
            );
            self.reconfigure_network().await;
        }
    }

    /// Responds to a heartbeat ping.
    async fn handle_ping(&mut self, sender_node: String, sequence: u64) {
        if sender_node == self.node_name {
            return;
        }

        if let Some(peer) = self.peers.get_mut(&sender_node) {
            peer.last_seen = Instant::now();
            peer.is_alive = true;
            peer.missed_heartbeats = 0;
        }

        let _ = self
            .ctx
            .send(ClusterPong {
                sender_node: self.node_name.clone(),
                sequence,
            })
            .await;
    }

    /// Records pong received from a peer.
    fn handle_pong(&mut self, sender_node: String, _sequence: u64) {
        if sender_node == self.node_name {
            return;
        }

        if let Some(peer) = self.peers.get_mut(&sender_node) {
            peer.last_seen = Instant::now();
            peer.is_alive = true;
            peer.missed_heartbeats = 0;
        }
    }

    /// Handles graceful or broadcasted departure of a node.
    fn handle_node_left(&mut self, node_name: String, reason: String) {
        if node_name == self.node_name {
            return;
        }

        if let Some(peer) = self.peers.get_mut(&node_name)
            && peer.is_alive
        {
            peer.is_alive = false;
            info!(
                node = %self.node_name,
                departed = %node_name,
                %reason,
                "Remote node departed cluster"
            );
        }
    }

    /// Reconfigures Elfo's `system.network` layer dynamically at runtime.
    ///
    /// Constructs an updated `Config` TOML string with the merged set of
    /// active transports in `discovery.predefined`, decodes it into `AnyConfig`,
    /// and sends `UpdateConfig` directly to `system.network`.
    ///
    /// `elfo_network/discovery` receives `ConfigUpdated`, calculates the diff,
    /// and automatically dials outbound TCP connections to all new peers.
    async fn reconfigure_network(&self) {
        let predefined_entries: Vec<String> = self
            .active_transports
            .iter()
            .map(|t| format!("\"{t}\""))
            .collect();

        let toml_content = format!(
            r#"
listen = ["{listen}"]
discovery.attempt_interval = "500ms"
discovery.predefined = [{predefined}]
compression.lz4 = "Preferred"
"#,
            listen = self.listen_addr,
            predefined = predefined_entries.join(", ")
        );

        match toml::from_str::<elfo::config::AnyConfig>(&toml_content) {
            Ok(any_config) => {
                info!(
                    node = %self.node_name,
                    peers = ?self.active_transports,
                    "Sending UpdateConfig to system.network with updated discovery peers"
                );
                let res = self
                    .ctx
                    .request_to(self.network_addr, UpdateConfig::new(any_config))
                    .resolve()
                    .await;
                if let Err(err) = res {
                    error!(%err, "Failed to apply dynamic network config update");
                }
            }
            Err(err) => {
                error!(%err, "Failed to parse dynamic network TOML configuration");
            }
        }
    }
}
