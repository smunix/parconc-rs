//! Distributed Chat Server executable.
//!
//! Launches a cluster node running elfo actor groups.

use clap::Parser;
use distrib_chat::build_topology;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Distributed Chat Server Node
#[derive(Parser, Debug)]
#[command(
    name = "distrib-chat",
    about = "Distributed Chat Server Node running Elfo actor groups",
    version
)]
struct Args {
    /// Node name corresponding to config/<NODE_NAME>.toml (e.g. us-east, ca-east, ca-west, us-west, eu)
    #[arg(value_name = "NODE_NAME", default_value = "us-east")]
    node_name: String,

    /// Chat client TCP port for incoming telnet / E2EE clients
    #[arg(value_name = "PORT", default_value_t = 44444)]
    tcp_port: u16,

    /// Explicit path to configuration file (defaults to config/<NODE_NAME>.toml)
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let node_name = args.node_name;
    let tcp_port = args.tcp_port;
    let config_path = args
        .config
        .unwrap_or_else(|| format!("config/{node_name}.toml"));

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
