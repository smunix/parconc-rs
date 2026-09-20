use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    process::{Child, Command},
    thread,
    time::Duration,
};

struct NodeProcess(Child);

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn test_gossip_seed_peering_dynamic_mesh() {
    let bin_path = env!("CARGO_BIN_EXE_distrib-chat");

    // 1. Spawn Seed Node (gossip_seed_us_east: TCP chat port 48101, cluster port 9601)
    let mut seed = NodeProcess(
        Command::new(bin_path)
            .arg("gossip_seed_us_east")
            .arg("48101")
            .spawn()
            .expect("failed to spawn gossip_seed_us_east"),
    );

    thread::sleep(Duration::from_millis(500));

    // 2. Spawn Dynamic Node 1 (gossip_dynamic_ca_west: chat port 48102, cluster port 9602)
    // Configured to connect ONLY to seed (9601); completely unaware of us_west (9603)
    let node_ca_west = Command::new(bin_path)
        .arg("gossip_dynamic_ca_west")
        .arg("48102")
        .spawn()
        .expect("failed to spawn gossip_dynamic_ca_west");
    let _ca_west_guard = NodeProcess(node_ca_west);

    thread::sleep(Duration::from_millis(500));

    // 3. Spawn Dynamic Node 2 (gossip_dynamic_us_west: chat port 48103, cluster port 9603)
    // Configured to connect ONLY to seed (9601); completely unaware of ca_west (9602)
    let node_us_west = Command::new(bin_path)
        .arg("gossip_dynamic_us_west")
        .arg("48103")
        .spawn()
        .expect("failed to spawn gossip_dynamic_us_west");
    let _us_west_guard = NodeProcess(node_us_west);

    // 4. Connect Alice to Dynamic Node 1 (ca_west on 48102)
    let mut alice =
        TcpStream::connect("127.0.0.1:48102").expect("failed to connect Alice to ca_west");
    alice
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut alice_reader = BufReader::new(alice.try_clone().unwrap());
    let mut line = String::new();

    alice_reader.read_line(&mut line).unwrap();
    assert!(line.contains("What is your name?"), "Got: {line}");

    alice.write_all(b"Alice\n").unwrap();
    alice.flush().unwrap();

    loop {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("Alice has connected") {
            break;
        }
    }

    // 5. Connect Bob to Dynamic Node 2 (us_west on 48103)
    let mut bob = TcpStream::connect("127.0.0.1:48103").expect("failed to connect Bob to us_west");
    bob.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut bob_reader = BufReader::new(bob.try_clone().unwrap());

    line.clear();
    bob_reader.read_line(&mut line).unwrap();
    assert!(line.contains("What is your name?"), "Got: {line}");

    bob.write_all(b"Bob\n").unwrap();
    bob.flush().unwrap();

    loop {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("Bob has connected") {
            break;
        }
    }

    // 6. Wait until dynamic gossip discovery and cluster sync complete:
    // ca_west and us_west learn of each other via the seed, reconfigure their elfo-network,
    // establish a direct TCP connection, and reconcile their client tables.
    thread::sleep(Duration::from_millis(1500));

    let mut mesh_synced = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(12) {
        alice.write_all(b"/users\n").unwrap();
        alice.flush().unwrap();

        for _ in 0..5 {
            line.clear();
            if alice_reader.read_line(&mut line).is_ok() && line.contains("Bob") {
                mesh_synced = true;
                break;
            }
        }
        if mesh_synced {
            break;
        }
        thread::sleep(Duration::from_millis(400));
    }
    assert!(
        mesh_synced,
        "Dynamic mesh discovery timeout: Alice never saw Bob in /users"
    );

    // 7. Test direct whisper from Alice (on ca_west) to Bob (on us_west)
    alice
        .write_all(b"/tell Bob Dynamic gossip discovery works across non-seed nodes!\n")
        .unwrap();
    alice.flush().unwrap();

    let mut tell_received = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        line.clear();
        if bob_reader.read_line(&mut line).is_ok()
            && line.contains("*Alice*: Dynamic gossip discovery works across non-seed nodes!")
        {
            tell_received = true;
            break;
        }
    }
    assert!(
        tell_received,
        "Bob on us_west failed to receive whisper from Alice on ca_west"
    );

    // Verify Bob sees Alice in /users
    bob.write_all(b"/users\n").unwrap();
    bob.flush().unwrap();

    let mut saw_alice = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        for _ in 0..5 {
            line.clear();
            if bob_reader.read_line(&mut line).is_ok() && line.contains("Alice") {
                saw_alice = true;
                break;
            }
        }
        if saw_alice {
            break;
        }
        thread::sleep(Duration::from_millis(300));
    }
    assert!(saw_alice, "Expected Alice in /users output, got: {line}");

    // 8. Resilience Test: Kill the Seed Node!
    // Since ca_west and us_west dynamically established a direct mesh,
    // direct inter-node routing continues seamlessly without the seed node.
    let _ = seed.0.kill();
    let _ = seed.0.wait();
    thread::sleep(Duration::from_millis(500));

    alice
        .write_all(b"/tell Bob Direct mesh remains alive without seed node!\n")
        .unwrap();
    alice.flush().unwrap();

    let mut direct_received = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        line.clear();
        if bob_reader.read_line(&mut line).is_ok()
            && line.contains("*Alice*: Direct mesh remains alive without seed node!")
        {
            direct_received = true;
            break;
        }
    }
    assert!(
        direct_received,
        "Failed direct communication between dynamic nodes after seed node shutdown"
    );
}
