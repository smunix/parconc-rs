//! Dedicated Terminal Client with End-to-End Encryption (E2EE).
//!
//! Features:
//! - Generates local ephemeral and long-term X25519 identity keypairs.
//! - Connects to distributed chat cluster nodes via TCP.
//! - Publishes public identity key to the cluster directory on login.
//! - Transparently performs Diffie-Hellman key exchange and ChaCha20-Poly1305 AEAD encryption.
//! - Intercepts incoming encrypted whispers (`*E2EE* <sender>: <ciphertext>`),
//!   authenticating and decrypting them locally.
//! - Intermediate cluster servers only see and route opaque base64 ciphertext.

use anyhow::Result;
use distrib_chat::crypto::{
    PublicKey, StaticSecret, decode_pubkey, decrypt_message, encode_pubkey, encrypt_message,
    generate_identity_keypair,
};
use std::{collections::HashMap, env};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

struct ClientContext {
    username: String,
    secret_key: StaticSecret,
    pubkey_b64: String,
    peer_keys: HashMap<String, PublicKey>,
    pending_whispers: HashMap<String, Vec<String>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let username = args
        .next()
        .expect("Usage: distrib-chat-client <nickname> [server_host:port]");
    let server_addr = args.next().unwrap_or_else(|| "127.0.0.1:44441".to_string());

    println!("============================================================");
    println!(" Connecting to Distributed Chat Cluster at {server_addr}...");

    // 1. Generate local cryptographic keypair
    let (secret_key, public_key) = generate_identity_keypair();
    let pubkey_b64 = encode_pubkey(&public_key);

    println!(" Generated local X25519 identity keypair.");
    println!(" Nickname:   {username}");
    println!(" Public Key: {pubkey_b64}");
    println!("============================================================");

    // 2. Connect to server
    let stream = match TcpStream::connect(&server_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {server_addr}: {e}");
            return Ok(());
        }
    };

    let (read_half, mut write_half) = stream.into_split();
    let mut server_reader = BufReader::new(read_half).lines();

    // 3. Complete login handshake:
    // Read prompt: "What is your name?"
    if let Some(prompt) = server_reader.next_line().await?
        && !prompt.contains("What is your name?")
    {
        eprintln!("Unexpected prompt: {prompt}");
    }

    // Send username along with public key for immediate registration
    let login_line = format!("{username} {pubkey_b64}\r\n");
    write_half.write_all(login_line.as_bytes()).await?;
    write_half.flush().await?;

    let mut ctx = ClientContext {
        username: username.clone(),
        secret_key,
        pubkey_b64,
        peer_keys: HashMap::new(),
        pending_whispers: HashMap::new(),
    };

    let stdin = tokio::io::stdin();
    let mut stdin_reader = BufReader::new(stdin).lines();

    println!("Type /help to view available commands.");

    loop {
        tokio::select! {
            server_line = server_reader.next_line() => {
                match server_line {
                    Ok(Some(line)) => {
                        handle_server_message(&line, &mut ctx, &mut write_half).await?;
                    }
                    Ok(None) => {
                        println!("\r\n*** Server disconnected.");
                        break;
                    }
                    Err(e) => {
                        println!("\r\n*** Connection error: {e}");
                        break;
                    }
                }
            }
            user_input = stdin_reader.next_line() => {
                match user_input {
                    Ok(Some(line)) => {
                        let should_quit = handle_user_input(&line, &mut ctx, &mut write_half).await?;
                        if should_quit {
                            break;
                        }
                    }
                    Ok(None) => {
                        // EOF on stdin (Ctrl-D)
                        break;
                    }
                    Err(e) => {
                        eprintln!("Stdin error: {e}");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Processes incoming messages received from the server TCP stream.
async fn handle_server_message<W: AsyncWriteExt + Unpin>(
    line: &str,
    ctx: &mut ClientContext,
    writer: &mut W,
) -> Result<()> {
    // Check if line is an incoming E2EE whisper:
    // Format sent by server: "*E2EE* <sender>: <ciphertext>"
    if let Some(rest) = line.strip_prefix("*E2EE* ")
        && let Some((sender, ciphertext)) = rest.split_once(": ")
    {
        let sender = sender.trim();
        let ciphertext = ciphertext.trim();

        match decrypt_message(&ctx.secret_key, ciphertext) {
            Ok(plaintext) => {
                println!("\x1b[1;32m*E2EE* {sender}\x1b[0m: {plaintext}");
            }
            Err(err) => {
                println!("\x1b[1;31m*** [E2EE Decryption Failed from {sender}: {err}]\x1b[0m");
            }
        }
        return Ok(());
    }

    // Check if line is a public key response:
    // Format: "*** KEY <user> <base64_pubkey>"
    if let Some(rest) = line.strip_prefix("*** KEY ") {
        let mut parts = rest.split_whitespace();
        if let (Some(target_user), Some(key_b64)) = (parts.next(), parts.next()) {
            match decode_pubkey(key_b64) {
                Ok(pk) => {
                    ctx.peer_keys.insert(target_user.to_string(), pk);
                    println!("*** Retrieved and cached E2EE public key for {target_user}.");

                    // Flush any pending whispers queued for this user
                    if let Some(pending) = ctx.pending_whispers.remove(target_user) {
                        for msg in pending {
                            if let Ok(ciphertext) = encrypt_message(&pk, &msg) {
                                let send_cmd = format!("/etell {target_user} {ciphertext}\r\n");
                                writer.write_all(send_cmd.as_bytes()).await?;
                                writer.flush().await?;
                                println!("\x1b[1;34m*E2EE* -> {target_user}\x1b[0m: {msg}");
                            }
                        }
                    }
                }
                Err(err) => {
                    eprintln!("*** Received invalid public key for {target_user}: {err}");
                }
            }
            return Ok(());
        }
    }

    // Check for key error response
    if line.contains("does not have an E2EE public key registered")
        || line.contains("is not connected")
    {
        println!("{line}");
        // Clear pending whispers
        ctx.pending_whispers.clear();
        return Ok(());
    }

    // Normal server output (notices, broadcasts, plain whispers)
    println!("{line}");
    Ok(())
}

/// Processes commands or text entered by the user at the terminal.
async fn handle_user_input<W: AsyncWriteExt + Unpin>(
    line: &str,
    ctx: &mut ClientContext,
    writer: &mut W,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }

    if trimmed == "/quit" {
        writer.write_all(b"/quit\r\n").await?;
        writer.flush().await?;
        return Ok(true);
    }

    if trimmed == "/help" {
        println!("============================================================");
        println!(" Client Commands:");
        println!("   /tell <user> <msg>   - Send an End-to-End Encrypted whisper");
        println!("   /plain <user> <msg>  - Send an unencrypted whisper");
        println!("   /users               - List connected users and E2EE status");
        println!("   /getkey <user>       - Fetch & cache user's E2EE public key");
        println!("   /keys                - Display cached peer public keys");
        println!("   /mykey               - Show your local identity public key");
        println!("   /kick <user>         - Kick a user from the chat");
        println!("   /quit                - Disconnect and exit");
        println!("   <message>            - Broadcast public message to all");
        println!("============================================================");
        return Ok(false);
    }

    if trimmed == "/mykey" {
        println!("Your public key (X25519 Base64): {}", ctx.pubkey_b64);
        return Ok(false);
    }

    if trimmed == "/keys" {
        println!("Cached Peer Public Keys:");
        if ctx.peer_keys.is_empty() {
            println!("  (none cached)");
        } else {
            for (user, pk) in &ctx.peer_keys {
                println!("  {user}: {}", encode_pubkey(pk));
            }
        }
        return Ok(false);
    }

    if let Some(rest) = trimmed.strip_prefix("/plain ") {
        let mut parts = rest.splitn(2, ' ');
        let target = parts.next().unwrap_or("");
        let msg = parts.next().unwrap_or("");
        if target.is_empty() || msg.is_empty() {
            println!("Usage: /plain <user> <message>");
        } else {
            let cmd = format!("/tell {target} {msg}\r\n");
            writer.write_all(cmd.as_bytes()).await?;
            writer.flush().await?;
        }
        return Ok(false);
    }

    if let Some(rest) = trimmed.strip_prefix("/tell ") {
        let mut parts = rest.splitn(2, ' ');
        let target = parts.next().unwrap_or("");
        let msg = parts.next().unwrap_or("");

        if target.is_empty() || msg.is_empty() {
            println!("Usage: /tell <user> <message>");
            return Ok(false);
        }

        if target == ctx.username {
            println!("*** You cannot whisper to yourself.");
            return Ok(false);
        }

        // Check if recipient key is cached
        if let Some(peer_pk) = ctx.peer_keys.get(target) {
            match encrypt_message(peer_pk, msg) {
                Ok(ciphertext) => {
                    let cmd = format!("/etell {target} {ciphertext}\r\n");
                    writer.write_all(cmd.as_bytes()).await?;
                    writer.flush().await?;
                    println!("\x1b[1;34m*E2EE* -> {target}\x1b[0m: {msg}");
                }
                Err(e) => {
                    eprintln!("*** Encryption error: {e}");
                }
            }
        } else {
            // Queue whisper and request key from server
            println!("*** Requesting E2EE public key for {target}...");
            ctx.pending_whispers
                .entry(target.to_string())
                .or_default()
                .push(msg.to_string());

            let getkey_cmd = format!("/getkey {target}\r\n");
            writer.write_all(getkey_cmd.as_bytes()).await?;
            writer.flush().await?;
        }
        return Ok(false);
    }

    // Passthrough other commands or public chat broadcast
    let formatted = format!("{trimmed}\r\n");
    writer.write_all(formatted.as_bytes()).await?;
    writer.flush().await?;

    Ok(false)
}
