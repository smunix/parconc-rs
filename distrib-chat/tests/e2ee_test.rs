use distrib_chat::crypto::{
    decode_pubkey, decrypt_message, encode_pubkey, encrypt_message, generate_identity_keypair,
};
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
fn test_cluster_end_to_end_encryption() {
    let bin_path = env!("CARGO_BIN_EXE_distrib-chat");

    // Spawn Node 1 (TCP chat port 46661, cluster port 9501)
    let node1 = Command::new(bin_path)
        .arg("e2ee_node1")
        .arg("46661")
        .spawn()
        .expect("failed to spawn e2ee_node1");
    let _node1_guard = NodeProcess(node1);

    // Give node 1 time to bind
    thread::sleep(Duration::from_millis(500));

    // Spawn Node 2 (TCP chat port 46662, cluster port 9502)
    let node2 = Command::new(bin_path)
        .arg("e2ee_node2")
        .arg("46662")
        .spawn()
        .expect("failed to spawn e2ee_node2");
    let _node2_guard = NodeProcess(node2);

    // Wait for cluster discovery to settle
    thread::sleep(Duration::from_secs(2));

    // Generate local cryptographic keypairs for Alice and Bob
    let (alice_secret, alice_public) = generate_identity_keypair();
    let alice_pub_b64 = encode_pubkey(&alice_public);

    let (bob_secret, bob_public) = generate_identity_keypair();
    let bob_pub_b64 = encode_pubkey(&bob_public);

    // 1. Connect Alice to Node 1 with public key registration
    let mut alice = TcpStream::connect("127.0.0.1:46661").expect("failed to connect to node 1");
    let mut alice_reader = BufReader::new(alice.try_clone().unwrap());
    let mut line = String::new();

    alice_reader.read_line(&mut line).unwrap();
    assert!(line.contains("What is your name?"), "Got: {line}");

    // Register name + base64 public key
    let alice_reg = format!("Alice {alice_pub_b64}\n");
    alice.write_all(alice_reg.as_bytes()).unwrap();
    alice.flush().unwrap();

    loop {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("Alice has connected") {
            break;
        }
    }

    // 2. Connect Bob to Node 2 with public key registration
    let mut bob = TcpStream::connect("127.0.0.1:46662").expect("failed to connect to node 2");
    let mut bob_reader = BufReader::new(bob.try_clone().unwrap());

    line.clear();
    bob_reader.read_line(&mut line).unwrap();
    assert!(line.contains("What is your name?"), "Got: {line}");

    let bob_reg = format!("Bob {bob_pub_b64}\n");
    bob.write_all(bob_reg.as_bytes()).unwrap();
    bob.flush().unwrap();

    loop {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if line.contains("Bob has connected") {
            break;
        }
    }

    // Give cluster time to synchronize key directories
    thread::sleep(Duration::from_millis(500));

    // 3. Alice verifies Bob in /users has [e2ee] badge
    alice.write_all(b"/users\n").unwrap();
    alice.flush().unwrap();
    let mut found_bob_e2ee = false;
    for _ in 0..10 {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if line.contains("Connected users: Bob [e2ee]") {
            found_bob_e2ee = true;
            break;
        }
    }
    assert!(found_bob_e2ee, "Bob was not shown with [e2ee] badge");

    // 4. Alice fetches Bob's public key from the cluster directory
    alice.write_all(b"/getkey Bob\n").unwrap();
    alice.flush().unwrap();
    let mut fetched_bob_key = None;
    for _ in 0..10 {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if let Some(rest) = line.strip_prefix("*** KEY Bob ") {
            fetched_bob_key = Some(rest.trim().to_string());
            break;
        }
    }
    let fetched_bob_key = fetched_bob_key.expect("Alice failed to fetch Bob's key");
    assert_eq!(fetched_bob_key, bob_pub_b64);
    let decoded_bob_pub = decode_pubkey(&fetched_bob_key).expect("valid decoded pubkey");
    assert_eq!(decoded_bob_pub.as_bytes(), bob_public.as_bytes());

    // 5. Bob fetches Alice's public key from Node 2
    bob.write_all(b"/getkey Alice\n").unwrap();
    bob.flush().unwrap();
    let mut fetched_alice_key = None;
    for _ in 0..10 {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if let Some(rest) = line.strip_prefix("*** KEY Alice ") {
            fetched_alice_key = Some(rest.trim().to_string());
            break;
        }
    }
    let fetched_alice_key = fetched_alice_key.expect("Bob failed to fetch Alice's key");
    assert_eq!(fetched_alice_key, alice_pub_b64);

    // 6. Alice encrypts whisper destined for Bob and sends /etell Bob <ciphertext>
    let secret_plaintext = "Deeply confidential message across cluster nodes!";
    let ciphertext = encrypt_message(&bob_public, secret_plaintext).expect("encryption succeeds");

    let etell_cmd = format!("/etell Bob {ciphertext}\n");
    alice.write_all(etell_cmd.as_bytes()).unwrap();
    alice.flush().unwrap();

    // 7. Bob receives opaque ciphertext on Node 2
    let mut received_ciphertext = None;
    for _ in 0..10 {
        line.clear();
        bob_reader.read_line(&mut line).unwrap();
        if let Some(rest) = line.strip_prefix("*E2EE* Alice: ") {
            received_ciphertext = Some(rest.trim().to_string());
            break;
        }
    }
    let received_ciphertext = received_ciphertext.expect("Bob did not receive E2EE whisper");
    assert_eq!(received_ciphertext, ciphertext);

    // 8. Bob decrypts with Bob's private key
    let decrypted =
        decrypt_message(&bob_secret, &received_ciphertext).expect("decryption succeeds");
    assert_eq!(decrypted, secret_plaintext);

    // 9. Verify server blindness & non-decryptability:
    // Alice's secret key cannot decrypt Bob's message
    assert!(
        decrypt_message(&alice_secret, &received_ciphertext).is_err(),
        "Alice's secret must not decrypt Bob's message"
    );

    // Tampering with the ciphertext fails AEAD authentication
    let mut tampered = received_ciphertext.clone().into_bytes();
    tampered[10] ^= 0x42;
    let tampered_str = String::from_utf8_lossy(&tampered);
    assert!(
        decrypt_message(&bob_secret, &tampered_str).is_err(),
        "Tampered ciphertext must fail AEAD Poly1305 verification"
    );

    // 10. Bidirectional encrypted reply from Bob to Alice
    let reply_plaintext = "Reply from Bob: E2EE verified across the cluster!";
    let reply_ciphertext =
        encrypt_message(&alice_public, reply_plaintext).expect("encryption succeeds");

    let reply_cmd = format!("/etell Alice {reply_ciphertext}\n");
    bob.write_all(reply_cmd.as_bytes()).unwrap();
    bob.flush().unwrap();

    let mut alice_received_ciphertext = None;
    for _ in 0..10 {
        line.clear();
        alice_reader.read_line(&mut line).unwrap();
        if let Some(rest) = line.strip_prefix("*E2EE* Bob: ") {
            alice_received_ciphertext = Some(rest.trim().to_string());
            break;
        }
    }
    let alice_received_ciphertext =
        alice_received_ciphertext.expect("Alice did not receive E2EE reply");
    assert_eq!(alice_received_ciphertext, reply_ciphertext);

    let decrypted_reply =
        decrypt_message(&alice_secret, &alice_received_ciphertext).expect("decryption succeeds");
    assert_eq!(decrypted_reply, reply_plaintext);
}
