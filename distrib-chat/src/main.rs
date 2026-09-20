//! Distributed Chat Server executable.
//!
//! Launches a cluster node running elfo actor groups.

use distrib_chat::build_topology;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let node_name = args.next().unwrap_or_else(|| "us-east".to_string());
    let tcp_port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(44444);

    let config_path = format!("config/{node_name}.toml");

    println!("============================================================");
    println!(" Starting Distributed Chat Node: {node_name}");
    println!(" Config path: {config_path}");
    println!(" Chat Client TCP port: {tcp_port}");
    println!(" Connect with: nc 127.0.0.1 {tcp_port}  (or telnet)");
    println!(" Connect with E2EE: distrib-chat-client <name> 127.0.0.1:{tcp_port}");
    println!("============================================================");

    let pending_sockets = Arc::new(Mutex::new(HashMap::new()));
    let topology = build_topology(&node_name, &config_path, tcp_port, pending_sockets);

    // Start elfo runtime and run until terminated
    elfo::init::start(topology).await;

    Ok(())
}
