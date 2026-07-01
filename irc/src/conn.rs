//! IRC socket lifecycle behind a small interface. Owns connect (TCP, optional
//! TLS), registration (`PASS`/`NICK`/`USER`), NickServ `IDENTIFY`, `433`
//! nick-collision retry, `JOIN`, `PING`/`PONG` keepalive, and line read/parse.
//! Exposes an inbound stream of parsed [`Message`]s and an outbound writer. No
//! agent logic lives here.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::config::IrcConfig;
use crate::proto::Message;

/// Inter-line pacing applied when sending multiple lines, to avoid flood kicks.
pub const SEND_PACING: Duration = Duration::from_millis(300);

/// Maximum number of nick-collision retries during registration before giving up
/// and letting `run()` back off and reconnect. Prevents unbounded NICK churn
/// against a hostile or misconfigured server that rejects every candidate nick.
pub const MAX_NICK_ATTEMPTS: u32 = 6;

/// Idle read timeout: if no line (not even a server PING) arrives within this
/// window, treat the link as dead and return so `run()` reconnects. Converts a
/// half-open TCP drop — which `read_line` would otherwise wait on forever — into
/// a normal reconnect. Sized well above a typical server PING interval.
pub const READ_TIMEOUT: Duration = Duration::from_secs(300);

/// The split halves of a connection: a line reader and a shared writer.
pub struct Connection {
    reader: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Sync + Unpin>>>,
    /// The nick we actually registered (may differ from config after `433`).
    pub nick: String,
}

/// A cloneable outbound handle for sending `PRIVMSG`s from elsewhere.
#[derive(Clone)]
pub struct Sender {
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Sync + Unpin>>>,
}

impl Sender {
    /// Send a raw IRC line (CRLF is appended).
    pub async fn send_raw(&self, line: &str) -> Result<()> {
        let mut w = self.writer.lock().await;
        w.write_all(line.as_bytes()).await?;
        w.write_all(b"\r\n").await?;
        w.flush().await?;
        Ok(())
    }

    /// Send a `PRIVMSG` to a target (channel or nick).
    pub async fn privmsg(&self, target: &str, text: &str) -> Result<()> {
        self.send_raw(&format!("PRIVMSG {target} :{text}")).await
    }
}

impl Connection {
    /// A cloneable sender bound to this connection's writer.
    pub fn sender(&self) -> Sender {
        Sender {
            writer: self.writer.clone(),
        }
    }

    /// Read and parse the next IRC line. Returns `Ok(None)` on clean EOF, and
    /// `Err` on a read error or an idle timeout (no line within [`READ_TIMEOUT`],
    /// indicating a half-open link) so the caller can reconnect.
    ///
    /// PINGs are answered transparently and skipped (never surfaced). A
    /// server-initiated NICK change for our own current nick updates the live
    /// [`Connection::nick`] so self-ignore stays correct after a forced rename.
    pub async fn next_message(&mut self) -> Result<Option<Message>> {
        loop {
            let mut line = String::new();
            let read = tokio::time::timeout(READ_TIMEOUT, self.reader.read_line(&mut line)).await;
            let n = match read {
                Ok(result) => result?,
                Err(_) => bail!("irc read timed out after {READ_TIMEOUT:?}; assuming dead link"),
            };
            if n == 0 {
                return Ok(None);
            }
            let Some(msg) = Message::parse(&line) else {
                continue;
            };
            if msg.command.eq_ignore_ascii_case("PING") {
                let token = msg.params.first().cloned().unwrap_or_default();
                self.sender().send_raw(&format!("PONG :{token}")).await?;
                continue;
            }
            if msg.command.eq_ignore_ascii_case("NICK") {
                self.apply_nick_change(&msg);
            }
            return Ok(Some(msg));
        }
    }

    /// If `msg` is a NICK change whose source is our current nick, adopt the new
    /// nick so classify()/self-ignore keeps recognizing our own messages after a
    /// services-forced rename (NickServ enforcement, SANICK, ghost/regain).
    fn apply_nick_change(&mut self, msg: &Message) {
        let Some(old) = msg.sender_nick() else {
            return;
        };
        if !old.eq_ignore_ascii_case(&self.nick) {
            return;
        }
        // The new nick is the single NICK parameter (plain or trailing).
        if let Some(new) = msg.params.first().filter(|n| !n.is_empty()) {
            tracing::info!(old = %self.nick, new = %new, "irc nick changed by server");
            self.nick = new.clone();
        }
    }
}

/// Connect, register, identify to NickServ, and join channels. Handles `433`
/// nick collisions by suffixing. Returns a ready [`Connection`].
pub async fn connect_and_register(cfg: &IrcConfig) -> Result<Connection> {
    let server = cfg
        .server
        .as_deref()
        .context("irc.server is required (set extensions.irc.server or IRC_SERVER)")?;
    let base_nick = cfg
        .nick
        .as_deref()
        .context("irc.nick is required (set extensions.irc.nick or IRC_NICK)")?;

    let (reader, writer): (
        Box<dyn AsyncRead + Send + Sync + Unpin>,
        Box<dyn AsyncWrite + Send + Sync + Unpin>,
    ) = open_stream(server, cfg.effective_port(), cfg.tls()).await?;

    let writer = Arc::new(Mutex::new(writer));
    let mut conn = Connection {
        reader: BufReader::new(reader),
        writer,
        nick: base_nick.to_string(),
    };

    // Registration handshake.
    if let Some(pass) = cfg.server_password.as_deref().filter(|p| !p.is_empty()) {
        conn.sender().send_raw(&format!("PASS {pass}")).await?;
    }
    let mut nick = base_nick.to_string();
    conn.sender().send_raw(&format!("NICK {nick}")).await?;
    conn.sender().send_raw(&format!(
        "USER {} 0 * :{}",
        cfg.effective_username(),
        cfg.effective_realname()
    ))
    .await?;

    // Drive the registration to 001 (RPL_WELCOME), retrying nick on 433.
    let registered = drive_registration(&mut conn, &mut nick, base_nick).await?;
    if !registered {
        bail!("irc registration ended before RPL_WELCOME (001)");
    }
    conn.nick = nick.clone();
    tracing::info!(nick = %nick, server, "irc registered");

    // NickServ IDENTIFY.
    if let Some(pw) = cfg.nickserv_password.as_deref().filter(|p| !p.is_empty()) {
        conn.sender().send_raw(&format!("PRIVMSG NickServ :IDENTIFY {pw}")).await?;
        tracing::info!("irc sent NickServ IDENTIFY");
    }

    // Join channels.
    for channel in cfg.channel_names() {
        conn.sender().send_raw(&format!("JOIN {channel}")).await?;
        tracing::info!(channel, "irc joining channel");
    }

    Ok(conn)
}

/// Consume registration lines until `001`, handling `433` by suffixing the nick.
/// Returns true once welcomed.
async fn drive_registration(conn: &mut Connection, nick: &mut String, base: &str) -> Result<bool> {
    let mut attempt = 0u32;
    loop {
        let mut line = String::new();
        let n = conn.reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(false);
        }
        let Some(msg) = Message::parse(&line) else {
            continue;
        };
        match msg.command.as_str() {
            "PING" => {
                let token = msg.params.first().cloned().unwrap_or_default();
                conn.sender().send_raw(&format!("PONG :{token}")).await?;
            }
            "001" => return Ok(true),
            // ERR_NICKNAMEINUSE / ERR_NICKCOLLISION / ERR_UNAVAILRESOURCE.
            "433" | "436" | "437" => {
                attempt += 1;
                if attempt > MAX_NICK_ATTEMPTS {
                    bail!("could not acquire a nick after {MAX_NICK_ATTEMPTS} attempts");
                }
                *nick = format!("{base}{}", suffix_for(attempt));
                tracing::warn!(attempt, new_nick = %nick, "irc nick collision; retrying");
                conn.sender().send_raw(&format!("NICK {nick}")).await?;
            }
            // Fatal registration errors.
            "464" | "465" => bail!("irc registration rejected: {}", msg.params.join(" ")),
            _ => {}
        }
    }
}

/// Suffix used for the Nth collision retry: `_`, `__`, ... then numeric.
fn suffix_for(attempt: u32) -> String {
    if attempt <= 3 {
        "_".repeat(attempt as usize)
    } else {
        format!("{}", attempt)
    }
}

/// Open a TCP stream, wrapping it in TLS when requested.
async fn open_stream(
    host: &str,
    port: u16,
    tls: bool,
) -> Result<(
    Box<dyn AsyncRead + Send + Sync + Unpin>,
    Box<dyn AsyncWrite + Send + Sync + Unpin>,
)> {
    let tcp = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("connecting to {host}:{port}"))?;
    tcp.set_nodelay(true).ok();

    if !tls {
        let (r, w) = tcp.into_split();
        return Ok((Box::new(r), Box::new(w)));
    }

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let domain = ServerName::try_from(host.to_string())
        .with_context(|| format!("invalid TLS server name '{host}'"))?;
    let stream = connector
        .connect(domain, tcp)
        .await
        .context("TLS handshake failed")?;
    let (r, w) = tokio::io::split(stream);
    Ok((Box::new(r), Box::new(w)))
}

/// Build a `Sender` over an in-memory duplex pipe for testing outbound delivery
/// without a live server. Returns the sender and the server-side half (read the
/// bot's outbound lines from it). Test-only helper shared across modules.
#[cfg(test)]
pub(crate) fn duplex_sender() -> (Sender, tokio::io::DuplexStream) {
    let (client, server) = tokio::io::duplex(8192);
    let sender = Sender {
        writer: Arc::new(Mutex::new(Box::new(client))),
    };
    (sender, server)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Message;

    #[test]
    fn collision_suffix_grows() {
        assert_eq!(suffix_for(1), "_");
        assert_eq!(suffix_for(2), "__");
        assert_eq!(suffix_for(3), "___");
        assert_eq!(suffix_for(4), "4");
    }

    /// Build a `Connection` over an in-memory duplex pipe for testing the
    /// read/registration paths without a live server. Returns the connection and
    /// the server-side half (write to it to feed the bot lines; read the bot's
    /// outbound lines from it).
    fn duplex_conn(nick: &str) -> (Connection, tokio::io::DuplexStream) {
        let (client, server) = tokio::io::duplex(8192);
        let (r, w) = tokio::io::split(client);
        let conn = Connection {
            reader: BufReader::new(Box::new(r)),
            writer: Arc::new(Mutex::new(Box::new(w))),
            nick: nick.to_string(),
        };
        (conn, server)
    }

    #[tokio::test]
    async fn registration_gives_up_after_max_nick_attempts() {
        // A server that answers every NICK with 433 must NOT loop forever: after
        // MAX_NICK_ATTEMPTS the registration bails so run() can back off.
        let (mut conn, server) = duplex_conn("darbot");
        // Feed an endless stream of 433s ahead of time (buffered in the pipe).
        let mut feed = server;
        tokio::spawn(async move {
            for _ in 0..(MAX_NICK_ATTEMPTS + 5) {
                let _ = feed
                    .write_all(b":srv 433 * darbot :Nickname is already in use\r\n")
                    .await;
            }
            // Keep the write half alive so reads block (don't EOF early).
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(feed);
        });

        let mut nick = "darbot".to_string();
        let result = drive_registration(&mut conn, &mut nick, "darbot").await;
        let err = result.expect_err("must bail after the attempt cap");
        assert!(
            err.to_string().contains("could not acquire a nick"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nick_change_for_own_nick_updates_live_nick() {
        let (mut conn, _server) = duplex_conn("darbot");
        let msg = Message::parse(":darbot!u@h NICK :darbot_").unwrap();
        conn.apply_nick_change(&msg);
        assert_eq!(conn.nick, "darbot_");
    }

    #[test]
    fn nick_change_for_another_nick_is_ignored() {
        let (mut conn, _server) = duplex_conn("darbot");
        let msg = Message::parse(":someoneelse!u@h NICK :newname").unwrap();
        conn.apply_nick_change(&msg);
        assert_eq!(conn.nick, "darbot");
    }

    #[tokio::test(start_paused = true)]
    async fn read_timeout_yields_error_on_silent_link() {
        // A connection that never receives a line must error out (not hang) so the
        // reconnect path runs. With the paused virtual clock, tokio auto-advances
        // time when the runtime is otherwise idle, so the READ_TIMEOUT fires
        // instantly instead of waiting 300 real seconds. The held `_server` keeps
        // the pipe open so only the timeout (not EOF) can resolve the read.
        let (mut conn, _server) = duplex_conn("darbot");
        let err = conn
            .next_message()
            .await
            .expect_err("silent link must time out");
        assert!(err.to_string().contains("timed out"), "unexpected: {err}");
    }
}
