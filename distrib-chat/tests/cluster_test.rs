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
fn test_distributed_chat_two_nodes() {
    let bin_path = env!("CARGO_BIN_EXE_distrib-chat");

    // Spawn Node 1 (TCP chat port 45551, cluster port 9401)
    let node1 = Command::new(bin_path)
        .arg("test_node1")
        .arg("45551")
        .spawn()
        .expect("failed to spawn test_node1");
    let _node1_guard = NodeProcess(node1);

    // Give node 1 time to bind before node 2 initiates discovery
    thread::sleep(Duration::from_millis(500));

    // Spawn Node 2 (TCP chat port 45552, cluster port 9402)
    let node2 = Command::new(bin_path)
        .arg("test_node2")
        .arg("45552")
        .spawn()
        .expect("failed to spawn test_node2");
    let _node2_guard = NodeProcess(node2);

    // Wait for nodes to initialize and connect to each other
    thread::sleep(Duration::from_secs(2));

    // Connect Alice to Node 1
    let mut alice = TcpStream::connect("127.0.0.1:45551").expect("failed to connect to node 1");
    let mut alice_reader = BufReader::new(alice.try_clone().unwrap());
    let mut line = String::new();

    // Expect "What is your name?"
    alice_reader.read_line(&mut line).unwrap();
    assert!(
        line.contains("What is your name?"),
        "Expected prompt, got: {line}"
    );

    // Register as Alice
    alice.write_all(b"Alice\n").unwrap();
    alice.flush().unwrap();

    loop {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("Alice has connected") {
            break;
        }
    }

    // Alice queries /users when alone
    alice.write_all(b"/users\n").unwrap();
    alice.flush().unwrap();
    loop {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("No other users are currently connected") {
            break;
        }
    }

    // Connect Bob to Node 2
    let mut bob = TcpStream::connect("127.0.0.1:45552").expect("failed to connect to node 2");
    let mut bob_reader = BufReader::new(bob.try_clone().unwrap());

    line.clear();
    bob_reader.read_line(&mut line).unwrap();
    assert!(
        line.contains("What is your name?"),
        "Expected prompt, got: {line}"
    );

    // Register as Bob
    bob.write_all(b"Bob\n").unwrap();
    bob.flush().unwrap();

    loop {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("Bob has connected") {
            break;
        }
    }

    // Allow cluster synchronization
    thread::sleep(Duration::from_millis(300));

    // Alice queries /users and should see Bob
    alice.write_all(b"/users\n").unwrap();
    alice.flush().unwrap();
    let mut found_bob_user = false;
    for _ in 0..5 {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("Connected users: Bob") {
            found_bob_user = true;
            break;
        }
    }
    assert!(found_bob_user, "Alice did not see Bob in /users");

    // Bob queries /users and should see Alice
    bob.write_all(b"/users\n").unwrap();
    bob.flush().unwrap();
    let mut found_alice_user = false;
    for _ in 0..5 {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("Connected users: Alice") {
            found_alice_user = true;
            break;
        }
    }
    assert!(found_alice_user, "Bob did not see Alice in /users");

    // Bob broadcasts a message across the cluster
    bob.write_all(b"Hello cluster from Bob!\n").unwrap();
    bob.flush().unwrap();

    // Alice should receive Bob's message
    let mut found_broadcast = false;
    for _ in 0..5 {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("<Bob>: Hello cluster from Bob!") {
            found_broadcast = true;
            break;
        }
    }
    assert!(found_broadcast, "Alice did not receive Bob's broadcast");

    // Alice whispers to Bob across nodes
    alice.write_all(b"/tell Bob secret whisper\n").unwrap();
    alice.flush().unwrap();

    // Bob receives whisper
    let mut found_whisper = false;
    for _ in 0..5 {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("*Alice*: secret whisper") {
            found_whisper = true;
            break;
        }
    }
    assert!(found_whisper, "Bob did not receive Alice's whisper");

    // Alice kicks Bob across nodes
    alice.write_all(b"/kick Bob\n").unwrap();
    alice.flush().unwrap();

    // Bob receives kick notice
    let mut found_kick = false;
    for _ in 0..5 {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("You have been kicked: kicked by Alice") {
            found_kick = true;
            break;
        }
    }
    assert!(found_kick, "Bob was not kicked");
}
