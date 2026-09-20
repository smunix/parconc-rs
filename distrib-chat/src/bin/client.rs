//! Dedicated Terminal Client with Ratatui TUI and End-to-End Encryption (E2EE).
//!
//! Features:
//! - Modern, responsive multi-pane Terminal User Interface (TUI) powered by Ratatui & Crossterm.
//! - Automatic local X25519 identity key generation (private key never leaves process memory).
//! - Automatic key registration on login and real-time cluster key directory lookup.
//! - Transparent ChaCha20-Poly1305 AEAD authenticated encryption & forward-secret ephemeral DH key agreement.
//! - Color-coded chat message stream with timestamps, E2EE status tags, and smooth scrolling.
//! - Live sidebar tracking online cluster users, E2EE status indicators, and cached peer keys.
//! - Graceful terminal setup and teardown with emergency panic hook restoration.

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use distrib_chat::crypto::{
    PublicKey, StaticSecret, decode_pubkey, decrypt_message, encode_pubkey, encrypt_message,
    generate_identity_keypair,
};
use futures::StreamExt;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Wrap},
};
use std::{
    collections::HashMap,
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::Mutex,
    time::interval,
};

/// Represents the classification and display style of a chat item.
#[derive(Clone, Debug)]
enum MessageKind {
    Public { from: String, text: String },
    EncryptedIncoming { from: String, text: String },
    EncryptedOutgoing { to: String, text: String },
    PlainWhisperIncoming { from: String, text: String },
    PlainWhisperOutgoing { to: String, text: String },
    Notice(String),
    Error(String),
    System(String),
}

/// A timestamped chat message item.
#[derive(Clone, Debug)]
struct ChatItem {
    timestamp: String,
    kind: MessageKind,
}

/// Represents a connected user displayed in the sidebar.
#[derive(Clone, Debug)]
struct UserEntry {
    name: String,
    has_e2ee: bool,
}

/// Core application state for the Ratatui TUI client.
struct App {
    username: String,
    server_addr: String,
    secret_key: StaticSecret,
    pubkey_b64: String,
    peer_keys: HashMap<String, PublicKey>,
    pending_whispers: HashMap<String, Vec<String>>,
    messages: Vec<ChatItem>,
    users: Vec<UserEntry>,
    input: String,
    cursor_pos: usize,
    scroll_offset: usize,
    auto_scroll: bool,
    should_quit: bool,
}

impl App {
    fn new(
        username: String,
        server_addr: String,
        secret_key: StaticSecret,
        pubkey_b64: String,
    ) -> Self {
        let mut app = Self {
            username: username.clone(),
            server_addr,
            secret_key,
            pubkey_b64,
            peer_keys: HashMap::new(),
            pending_whispers: HashMap::new(),
            messages: Vec::new(),
            users: vec![UserEntry {
                name: username,
                has_e2ee: true,
            }],
            input: String::new(),
            cursor_pos: 0,
            scroll_offset: 0,
            auto_scroll: true,
            should_quit: false,
        };

        app.add_message(MessageKind::System(
            "Welcome to the Distributed Chat E2EE Terminal! Type /help for commands.".to_string(),
        ));
        app
    }

    fn current_timestamp() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        let hours = (now % 86400) / 3600;
        let minutes = (now % 3600) / 60;
        let seconds = now % 60;
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }

    fn add_message(&mut self, kind: MessageKind) {
        self.messages.push(ChatItem {
            timestamp: Self::current_timestamp(),
            kind,
        });
        if self.auto_scroll {
            self.scroll_offset = self.messages.len().saturating_sub(1);
        }
    }

    fn scroll_up(&mut self) {
        self.auto_scroll = false;
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    fn scroll_down(&mut self) {
        if self.scroll_offset + 1 < self.messages.len() {
            self.scroll_offset += 1;
        } else {
            self.auto_scroll = true;
        }
    }

    fn scroll_page_up(&mut self) {
        self.auto_scroll = false;
        self.scroll_offset = self.scroll_offset.saturating_sub(10);
    }

    fn scroll_page_down(&mut self) {
        if self.scroll_offset + 10 < self.messages.len() {
            self.scroll_offset += 10;
        } else {
            self.scroll_offset = self.messages.len().saturating_sub(1);
            self.auto_scroll = true;
        }
    }

    fn insert_char(&mut self, c: char) {
        self.input.insert(self.cursor_pos, c);
        self.cursor_pos += 1;
    }

    fn delete_char(&mut self) {
        if self.cursor_pos > 0 {
            self.cursor_pos -= 1;
            self.input.remove(self.cursor_pos);
        }
    }

    fn move_cursor_left(&mut self) {
        self.cursor_pos = self.cursor_pos.saturating_sub(1);
    }

    fn move_cursor_right(&mut self) {
        if self.cursor_pos < self.input.len() {
            self.cursor_pos += 1;
        }
    }
}

/// Dedicated Terminal Client with Ratatui TUI and End-to-End Encryption (E2EE)
#[derive(Parser, Debug)]
#[command(
    name = "distrib-chat-client",
    about = "Dedicated Terminal Client with Ratatui TUI and End-to-End Encryption (E2EE)",
    version
)]
struct Args {
    /// Nickname / username for the chat session
    #[arg(value_name = "NICKNAME")]
    username: String,

    /// Chat server address in host:port format
    #[arg(value_name = "SERVER", default_value = "127.0.0.1:44441")]
    server: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let username = args.username;
    let server_addr = args.server;

    // 1. Generate local cryptographic keypair
    let (secret_key, public_key) = generate_identity_keypair();
    let pubkey_b64 = encode_pubkey(&public_key);

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

    // Initial query to populate users list
    write_half.write_all(b"/users\r\n").await?;
    write_half.flush().await?;

    let app = Arc::new(Mutex::new(App::new(
        username,
        server_addr,
        secret_key,
        pubkey_b64,
    )));

    // 4. Setup terminal and emergency panic hook
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(panic_info);
    }));

    // 5. Main asynchronous event loop
    let mut event_stream = EventStream::new();
    let mut sync_ticker = interval(Duration::from_secs(5));

    loop {
        // Render UI
        {
            let app_guard = app.lock().await;
            terminal.draw(|f| render_ui(f, &app_guard))?;
            if app_guard.should_quit {
                break;
            }
        }

        tokio::select! {
            // Periodic background sync of online users
            _ = sync_ticker.tick() => {
                let _ = write_half.write_all(b"/users\r\n").await;
                let _ = write_half.flush().await;
            }

            // Incoming messages from the chat server
            server_line = server_reader.next_line() => {
                match server_line {
                    Ok(Some(line)) => {
                        handle_server_message(&line, &app, &mut write_half).await?;
                    }
                    Ok(None) => {
                        let mut app_guard = app.lock().await;
                        app_guard.add_message(MessageKind::Error("Server disconnected.".to_string()));
                        app_guard.should_quit = true;
                    }
                    Err(e) => {
                        let mut app_guard = app.lock().await;
                        app_guard.add_message(MessageKind::Error(format!("Connection error: {e}")));
                        app_guard.should_quit = true;
                    }
                }
            }

            // Terminal input events (keys, resize)
            maybe_event = event_stream.next() => {
                if let Some(Ok(Event::Key(key_event))) = maybe_event {
                    handle_key_event(key_event, &app, &mut write_half).await?;
                }
            }
        }
    }

    // 6. Restore terminal on exit
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

/// Renders the complete modern TUI interface.
fn render_ui(f: &mut ratatui::Frame, app: &App) {
    let size = f.area();

    // Vertical layout: Header (3) -> Main Body (Min 5) -> Input (3) -> Footer (1)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(size);

    // -------------------------------------------------------------------------
    // 1. Header Bar
    // -------------------------------------------------------------------------
    let key_preview = if app.pubkey_b64.len() >= 12 {
        format!("{}...", &app.pubkey_b64[..12])
    } else {
        app.pubkey_b64.clone()
    };

    let header_text = vec![
        Line::from(vec![
            Span::styled(
                "  DISTRIBUTED CHAT  ",
                Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
            ),
            Span::styled(
                "  [End-to-End Encrypted (E2EE) Actor Mesh] ",
                Style::default().fg(Color::Cyan).bold(),
            ),
        ]),
        Line::from(vec![
            Span::raw("  User: "),
            Span::styled(&app.username, Style::default().fg(Color::Yellow).bold()),
            Span::raw("  |  Server: "),
            Span::styled(&app.server_addr, Style::default().fg(Color::Green)),
            Span::raw("  |  X25519 Key: "),
            Span::styled(key_preview, Style::default().fg(Color::DarkGray)),
            Span::raw("  |  Status: "),
            Span::styled("● Connected", Style::default().fg(Color::Green).bold()),
        ]),
    ];

    let header_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));
    let header_paragraph = Paragraph::new(header_text).block(header_block);
    f.render_widget(header_paragraph, chunks[0]);

    // -------------------------------------------------------------------------
    // 2. Main Body: Messages (Left 75%) + Sidebar (Right 25%)
    // -------------------------------------------------------------------------
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(75), Constraint::Percentage(25)])
        .split(chunks[1]);

    // Left Pane: Messages
    let message_items: Vec<ListItem> = app
        .messages
        .iter()
        .map(|item| {
            let time_span = Span::styled(
                format!("[{}] ", item.timestamp),
                Style::default().fg(Color::DarkGray),
            );

            let line = match &item.kind {
                MessageKind::Public { from, text } => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("<{from}>"),
                        Style::default().fg(Color::LightCyan).bold(),
                    ),
                    Span::raw(format!(": {text}")),
                ]),
                MessageKind::EncryptedIncoming { from, text } => Line::from(vec![
                    time_span,
                    Span::styled(
                        "[E2EE]",
                        Style::default().fg(Color::Black).bg(Color::Green).bold(),
                    ),
                    Span::styled(
                        format!(" {from}: "),
                        Style::default().fg(Color::LightGreen).bold(),
                    ),
                    Span::styled(text, Style::default().fg(Color::White)),
                ]),
                MessageKind::EncryptedOutgoing { to, text } => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("[E2EE -> {to}]"),
                        Style::default().fg(Color::Black).bg(Color::Magenta).bold(),
                    ),
                    Span::styled(format!(": {text}"), Style::default().fg(Color::White)),
                ]),
                MessageKind::PlainWhisperIncoming { from, text } => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("*{from}*"),
                        Style::default().fg(Color::LightBlue).bold(),
                    ),
                    Span::styled(format!(": {text}"), Style::default().fg(Color::White)),
                ]),
                MessageKind::PlainWhisperOutgoing { to, text } => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("*You -> {to}*"),
                        Style::default().fg(Color::LightBlue).bold(),
                    ),
                    Span::styled(format!(": {text}"), Style::default().fg(Color::White)),
                ]),
                MessageKind::Notice(msg) => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("*** {msg}"),
                        Style::default().fg(Color::Yellow).italic(),
                    ),
                ]),
                MessageKind::Error(err) => Line::from(vec![
                    time_span,
                    Span::styled(format!("*** {err}"), Style::default().fg(Color::Red).bold()),
                ]),
                MessageKind::System(sys) => Line::from(vec![
                    time_span,
                    Span::styled(
                        format!("[System] {sys}"),
                        Style::default().fg(Color::LightYellow),
                    ),
                ]),
            };

            ListItem::new(line)
        })
        .collect();

    let scroll_msg = if app.auto_scroll {
        "Auto-Scroll"
    } else {
        "Manual Scroll"
    };

    let messages_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(format!(
            " Messages ({}) [{scroll_msg}] ",
            app.messages.len()
        ))
        .title_alignment(Alignment::Left)
        .border_style(Style::default().fg(Color::White));

    // Compute visible window for scrolling
    let visible_height = body_chunks[0].height.saturating_sub(2) as usize;
    let total_items = message_items.len();
    let start_idx = if total_items <= visible_height {
        0
    } else if app.auto_scroll {
        total_items.saturating_sub(visible_height)
    } else {
        app.scroll_offset
            .min(total_items.saturating_sub(visible_height))
    };

    let slice: Vec<ListItem> = message_items
        .into_iter()
        .skip(start_idx)
        .take(visible_height)
        .collect();
    let messages_list = List::new(slice).block(messages_block);
    f.render_widget(messages_list, body_chunks[0]);

    // Right Pane: Sidebar (Online Users 60% + Key Directory 40%)
    let sidebar_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(body_chunks[1]);

    // Top Sidebar: Online Users
    let user_items: Vec<ListItem> = app
        .users
        .iter()
        .map(|u| {
            let is_me = u.name == app.username;
            let dot = if is_me {
                Span::styled("● ", Style::default().fg(Color::Yellow))
            } else if u.has_e2ee {
                Span::styled("● ", Style::default().fg(Color::Green))
            } else {
                Span::styled("○ ", Style::default().fg(Color::Gray))
            };

            let name_style = if is_me {
                Style::default().fg(Color::Yellow).bold()
            } else {
                Style::default().fg(Color::White)
            };

            let badge = if u.has_e2ee {
                Span::styled(" [E2EE]", Style::default().fg(Color::LightGreen))
            } else {
                Span::styled(" [plain]", Style::default().fg(Color::DarkGray))
            };

            let me_tag = if is_me {
                Span::styled(" (you)", Style::default().fg(Color::DarkGray))
            } else {
                Span::raw("")
            };

            ListItem::new(Line::from(vec![
                dot,
                Span::styled(&u.name, name_style),
                badge,
                me_tag,
            ]))
        })
        .collect();

    let users_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(format!(" Online Users ({}) ", app.users.len()))
        .border_style(Style::default().fg(Color::Green));
    let users_list = List::new(user_items).block(users_block);
    f.render_widget(users_list, sidebar_chunks[0]);

    // Bottom Sidebar: Key Directory
    let mut key_lines = Vec::new();
    key_lines.push(Line::from(vec![
        Span::raw("Cached Keys: "),
        Span::styled(
            format!("{}", app.peer_keys.len()),
            Style::default().fg(Color::Green).bold(),
        ),
    ]));

    for peer in app.peer_keys.keys() {
        key_lines.push(Line::from(vec![
            Span::styled("✔ ", Style::default().fg(Color::Green)),
            Span::styled(peer, Style::default().fg(Color::Cyan)),
        ]));
    }

    if app.peer_keys.is_empty() {
        key_lines.push(Line::from(Span::styled(
            "No peer keys cached yet.",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let keys_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(" Key Directory ")
        .border_style(Style::default().fg(Color::Blue));
    let keys_paragraph = Paragraph::new(key_lines)
        .block(keys_block)
        .wrap(Wrap { trim: true });
    f.render_widget(keys_paragraph, sidebar_chunks[1]);

    // -------------------------------------------------------------------------
    // 3. Input Box
    // -------------------------------------------------------------------------
    let input_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(" Message / Command ")
        .border_style(Style::default().fg(Color::Yellow));

    let input_text = Paragraph::new(format!("> {}", app.input)).block(input_block);
    f.render_widget(input_text, chunks[2]);

    // Draw cursor in input box
    let cursor_x = chunks[2].x + 3 + app.cursor_pos as u16;
    let cursor_y = chunks[2].y + 1;
    if cursor_x < chunks[2].x + chunks[2].width.saturating_sub(1) {
        f.set_cursor_position((cursor_x, cursor_y));
    }

    // -------------------------------------------------------------------------
    // 4. Footer Bar
    // -------------------------------------------------------------------------
    let footer_text = Line::from(vec![
        Span::styled(
            " [Enter] ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightBlue)
                .bold(),
        ),
        Span::raw(" Send  "),
        Span::styled(
            " [/tell <user> <msg>] ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightGreen)
                .bold(),
        ),
        Span::raw(" E2EE Whisper  "),
        Span::styled(
            " [/users] ",
            Style::default().fg(Color::Black).bg(Color::Yellow).bold(),
        ),
        Span::raw(" Refresh  "),
        Span::styled(
            " [PgUp/PgDn] ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" Scroll  "),
        Span::styled(
            " [Esc] ",
            Style::default().fg(Color::Black).bg(Color::Red).bold(),
        ),
        Span::raw(" Quit"),
    ]);

    let footer_paragraph = Paragraph::new(footer_text).alignment(Alignment::Center);
    f.render_widget(footer_paragraph, chunks[3]);
}

/// Handles incoming server lines and updates UI state.
async fn handle_server_message<W: AsyncWriteExt + Unpin>(
    line: &str,
    app: &Arc<Mutex<App>>,
    writer: &mut W,
) -> Result<()> {
    // 1. E2EE Whisper
    if let Some(rest) = line.strip_prefix("*E2EE* ")
        && let Some((sender, ciphertext)) = rest.split_once(": ")
    {
        let sender = sender.trim().to_string();
        let ciphertext = ciphertext.trim();

        let mut app_guard = app.lock().await;
        match decrypt_message(&app_guard.secret_key, ciphertext) {
            Ok(plaintext) => {
                app_guard.add_message(MessageKind::EncryptedIncoming {
                    from: sender,
                    text: plaintext,
                });
            }
            Err(err) => {
                app_guard.add_message(MessageKind::Error(format!(
                    "E2EE Decryption Failed from {sender}: {err}"
                )));
            }
        }
        return Ok(());
    }

    // 2. Public key response
    if let Some(rest) = line.strip_prefix("*** KEY ") {
        let mut parts = rest.split_whitespace();
        if let (Some(target_user), Some(key_b64)) = (parts.next(), parts.next()) {
            let target_user = target_user.to_string();
            match decode_pubkey(key_b64) {
                Ok(pk) => {
                    let mut app_guard = app.lock().await;
                    app_guard.peer_keys.insert(target_user.clone(), pk);
                    app_guard.add_message(MessageKind::Notice(format!(
                        "Retrieved and cached E2EE public key for {target_user}."
                    )));

                    // Flush pending whispers queued for this user
                    if let Some(pending) = app_guard.pending_whispers.remove(&target_user) {
                        for msg in pending {
                            if let Ok(ciphertext) = encrypt_message(&pk, &msg) {
                                let send_cmd = format!("/etell {target_user} {ciphertext}\r\n");
                                let _ = writer.write_all(send_cmd.as_bytes()).await;
                                let _ = writer.flush().await;
                                app_guard.add_message(MessageKind::EncryptedOutgoing {
                                    to: target_user.clone(),
                                    text: msg,
                                });
                            }
                        }
                    }
                }
                Err(err) => {
                    let mut app_guard = app.lock().await;
                    app_guard.add_message(MessageKind::Error(format!(
                        "Invalid public key received for {target_user}: {err}"
                    )));
                }
            }
            return Ok(());
        }
    }

    // 3. User list response
    if let Some(rest) = line.strip_prefix("*** Connected users: ") {
        let mut app_guard = app.lock().await;
        let mut parsed_users = Vec::new();

        // Always include self first
        parsed_users.push(UserEntry {
            name: app_guard.username.clone(),
            has_e2ee: true,
        });

        for user_str in rest.split(',') {
            let trimmed = user_str.trim();
            if trimmed.is_empty() {
                continue;
            }

            let (name, has_e2ee) = if let Some(n) = trimmed.strip_suffix(" [e2ee]") {
                (n.trim().to_string(), true)
            } else {
                (trimmed.to_string(), false)
            };

            if name != app_guard.username {
                parsed_users.push(UserEntry { name, has_e2ee });
            }
        }

        app_guard.users = parsed_users;
        return Ok(());
    }

    if line.contains("No other users are currently connected") {
        let mut app_guard = app.lock().await;
        let my_name = app_guard.username.clone();
        app_guard.users = vec![UserEntry {
            name: my_name,
            has_e2ee: true,
        }];
        return Ok(());
    }

    // 4. Public broadcast: "<Alice>: hello"
    if line.starts_with('<')
        && let Some((from, text)) = line.strip_prefix('<').and_then(|s| s.split_once(">: "))
    {
        let mut app_guard = app.lock().await;
        app_guard.add_message(MessageKind::Public {
            from: from.to_string(),
            text: text.to_string(),
        });
        return Ok(());
    }

    // 5. Plain whisper: "*Alice*: hello"
    if line.starts_with('*')
        && !line.starts_with("***")
        && let Some((from, text)) = line.strip_prefix('*').and_then(|s| s.split_once("*: "))
    {
        let mut app_guard = app.lock().await;
        app_guard.add_message(MessageKind::PlainWhisperIncoming {
            from: from.to_string(),
            text: text.to_string(),
        });
        return Ok(());
    }

    // 6. Connect / Disconnect notices
    if line.contains("has connected") || line.contains("has disconnected") {
        let _ = writer.write_all(b"/users\r\n").await;
        let _ = writer.flush().await;
    }

    // 7. General notices or errors
    let mut app_guard = app.lock().await;
    if let Some(notice) = line.strip_prefix("*** ") {
        if notice.starts_with("Error") || notice.contains("not connected") {
            app_guard.add_message(MessageKind::Error(notice.to_string()));
        } else {
            app_guard.add_message(MessageKind::Notice(notice.to_string()));
        }
    } else {
        app_guard.add_message(MessageKind::Notice(line.to_string()));
    }

    Ok(())
}

/// Handles keyboard input from the user.
async fn handle_key_event<W: AsyncWriteExt + Unpin>(
    key: KeyEvent,
    app: &Arc<Mutex<App>>,
    writer: &mut W,
) -> Result<()> {
    let mut app_guard = app.lock().await;

    match key.code {
        KeyCode::Esc => {
            app_guard.should_quit = true;
            let _ = writer.write_all(b"/quit\r\n").await;
            let _ = writer.flush().await;
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app_guard.should_quit = true;
            let _ = writer.write_all(b"/quit\r\n").await;
            let _ = writer.flush().await;
        }
        KeyCode::Enter => {
            let line = std::mem::take(&mut app_guard.input);
            app_guard.cursor_pos = 0;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return Ok(());
            }

            if trimmed == "/quit" {
                app_guard.should_quit = true;
                let _ = writer.write_all(b"/quit\r\n").await;
                let _ = writer.flush().await;
                return Ok(());
            }

            if trimmed == "/help" {
                app_guard.add_message(MessageKind::System(
                    "--- Available Client Commands ---".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/tell <user> <msg>   - Send an End-to-End Encrypted whisper (transparent key exchange)".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/plain <user> <msg>  - Send an unencrypted whisper".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/users               - Refresh online users across the cluster".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/getkey <user>       - Query and cache a user's E2EE public key".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/keys                - Display all cached peer public keys".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/mykey               - Display your own local X25519 public key".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/kick <user>         - Kick a user from the chat server".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "/quit                - Disconnect and exit".to_string(),
                ));
                app_guard.add_message(MessageKind::System(
                    "<message>            - Broadcast public message to all users".to_string(),
                ));
                return Ok(());
            }

            if trimmed == "/mykey" {
                let key = app_guard.pubkey_b64.clone();
                app_guard.add_message(MessageKind::System(format!(
                    "Your X25519 Public Key: {key}"
                )));
                return Ok(());
            }

            if trimmed == "/keys" {
                if app_guard.peer_keys.is_empty() {
                    app_guard.add_message(MessageKind::System("No cached peer keys.".to_string()));
                } else {
                    let keys: Vec<(String, String)> = app_guard
                        .peer_keys
                        .iter()
                        .map(|(peer, pk)| (peer.clone(), encode_pubkey(pk)))
                        .collect();
                    app_guard
                        .add_message(MessageKind::System("--- Cached Peer Keys ---".to_string()));
                    for (peer, pk_encoded) in keys {
                        app_guard.add_message(MessageKind::System(format!("{peer}: {pk_encoded}")));
                    }
                }
                return Ok(());
            }

            if let Some(rest) = trimmed.strip_prefix("/plain ") {
                let mut parts = rest.splitn(2, ' ');
                let target = parts.next().unwrap_or("");
                let msg = parts.next().unwrap_or("");
                if target.is_empty() || msg.is_empty() {
                    app_guard.add_message(MessageKind::Error(
                        "Usage: /plain <user> <message>".to_string(),
                    ));
                } else {
                    let cmd = format!("/tell {target} {msg}\r\n");
                    let _ = writer.write_all(cmd.as_bytes()).await;
                    let _ = writer.flush().await;
                    app_guard.add_message(MessageKind::PlainWhisperOutgoing {
                        to: target.to_string(),
                        text: msg.to_string(),
                    });
                }
                return Ok(());
            }

            if let Some(rest) = trimmed.strip_prefix("/tell ") {
                let mut parts = rest.splitn(2, ' ');
                let target = parts.next().unwrap_or("");
                let msg = parts.next().unwrap_or("");

                if target.is_empty() || msg.is_empty() {
                    app_guard.add_message(MessageKind::Error(
                        "Usage: /tell <user> <message>".to_string(),
                    ));
                    return Ok(());
                }

                if target == app_guard.username {
                    app_guard.add_message(MessageKind::Error(
                        "You cannot whisper to yourself.".to_string(),
                    ));
                    return Ok(());
                }

                // Check if target public key is cached
                if let Some(peer_pk) = app_guard.peer_keys.get(target) {
                    match encrypt_message(peer_pk, msg) {
                        Ok(ciphertext) => {
                            let cmd = format!("/etell {target} {ciphertext}\r\n");
                            let _ = writer.write_all(cmd.as_bytes()).await;
                            let _ = writer.flush().await;
                            app_guard.add_message(MessageKind::EncryptedOutgoing {
                                to: target.to_string(),
                                text: msg.to_string(),
                            });
                        }
                        Err(e) => {
                            app_guard
                                .add_message(MessageKind::Error(format!("Encryption error: {e}")));
                        }
                    }
                } else {
                    // Queue whisper and request key from server
                    app_guard.add_message(MessageKind::System(format!(
                        "Requesting E2EE public key for {target}..."
                    )));
                    app_guard
                        .pending_whispers
                        .entry(target.to_string())
                        .or_default()
                        .push(msg.to_string());

                    let getkey_cmd = format!("/getkey {target}\r\n");
                    let _ = writer.write_all(getkey_cmd.as_bytes()).await;
                    let _ = writer.flush().await;
                }
                return Ok(());
            }

            // Passthrough commands or public broadcast
            let formatted = format!("{trimmed}\r\n");
            let _ = writer.write_all(formatted.as_bytes()).await;
            let _ = writer.flush().await;
        }
        KeyCode::Char(c) => {
            app_guard.insert_char(c);
        }
        KeyCode::Backspace => {
            app_guard.delete_char();
        }
        KeyCode::Left => {
            app_guard.move_cursor_left();
        }
        KeyCode::Right => {
            app_guard.move_cursor_right();
        }
        KeyCode::Up => {
            app_guard.scroll_up();
        }
        KeyCode::Down => {
            app_guard.scroll_down();
        }
        KeyCode::PageUp => {
            app_guard.scroll_page_up();
        }
        KeyCode::PageDown => {
            app_guard.scroll_page_down();
        }
        _ => {}
    }

    Ok(())
}
