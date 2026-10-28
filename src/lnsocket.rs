use crate::{
    Error,
    ln::{
        msgs::{self, DecodeError},
        peer_channel_encryptor::PeerChannelEncryptor,
        wire::{self, Message},
    },
    proxy::{LnStream, TorConfig},
    socket_addr::SocketAddress,
    util::ser::Writeable,
};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey, rand};
use std::io::{self, Cursor};
use std::net::ToSocketAddrs;
use std::str::FromStr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;
use tokio_socks::tcp::Socks5Stream;

const ACT_TWO_SIZE: usize = 50;

struct ReconnectData {
    our_key: SecretKey,
    their_pubkey: PublicKey,
    addr: String,
}

/// A Lightning Network TCP socket that performs the BOLT 8 Noise handshake and message encryption.
///
/// [`LNSocket`] wraps a `tokio::net::TcpStream` with Noise state (via [`PeerChannelEncryptor`])
/// to handle encrypted Lightning messages over TCP.
///
/// # Typical usage
/// ```no_run
/// use bitcoin::secp256k1::{SecretKey, PublicKey, rand};
/// use lnsocket::LNSocket;
/// use lnsocket::ln::msgs;
///
/// # async fn example(peer: PublicKey) -> Result<(), lnsocket::Error> {
/// let sk = SecretKey::new(&mut rand::thread_rng());
/// let mut sock = LNSocket::connect_and_init(sk, peer, "node.example.com:9735").await?;
/// sock.write(&msgs::Ping { ponglen: 4, byteslen: 8 }).await?;
/// let msg = sock.read().await?;
/// # Ok(()) }
/// ```
///
/// ⚠️ This type does **not** do retries/keepalive; see [`CommandoClient`] if you want managed reconnects.
pub struct LNSocket {
    channel: PeerChannelEncryptor,
    stream: LnStream,
    reconnect: ReconnectData,
    frame: PartialFrame,
}

/// The incoming frame read so far. It lives on the socket rather than in the read future, so a
/// [`LNSocket::read_custom`] dropped partway through (as a `tokio::select!` branch that lost)
/// leaves the bytes it had read, and the length header it had decrypted, for the next call.
#[derive(Default)]
struct PartialFrame {
    /// Bytes of the current stage: the 18-byte length header, then the body and its MAC.
    buf: Vec<u8>,
    /// How much of `buf` has been read off the stream.
    filled: usize,
    /// The body's length once the header has been decrypted. Decrypting moves the noise
    /// receive nonce, so it happens exactly once per frame.
    body_len: Option<usize>,
}

impl LNSocket {
    /// Connect to a Lightning peer and complete the BOLT 8 Noise handshake.
    ///
    /// Resolves the given `addr`, establishes a TCP connection, and performs act1/act2/act3
    /// handshake using `our_key` and the peer's public key.
    ///
    /// Does **not** send or expect an `init` message.
    /// Use [`LNSocket::connect_and_init`] if you want handshake + `init` exchange.
    pub async fn connect(
        our_key: SecretKey,
        their_pubkey: PublicKey,
        addr: &str,
    ) -> Result<LNSocket, Error> {
        Self::connect_with_tor_config(our_key, their_pubkey, addr, None).await
    }

    /// Connect to a Lightning peer with optional TOR proxy configuration.
    ///
    /// Resolves the given `addr`, establishes a TCP connection, and performs act1/act2/act3
    /// handshake using `our_key` and the peer's public key.
    ///
    /// For .onion addresses, uses the provided `tor_config` or defaults to localhost:9050.
    /// For clearnet addresses, ignores the `tor_config` and connects directly.
    ///
    /// Does **not** send or expect an `init` message.
    /// Use [`LNSocket::connect_and_init_with_tor_config`] if you want handshake + `init` exchange.
    pub async fn connect_with_tor_config(
        our_key: SecretKey,
        their_pubkey: PublicKey,
        addr: &str,
        tor_config: Option<TorConfig>,
    ) -> Result<LNSocket, Error> {
        let secp_ctx = Secp256k1::signing_only();

        // Parse the address string into a structured SocketAddress
        let socket_addr = SocketAddress::from_str(addr)?;

        let mut stream = match &socket_addr {
            SocketAddress::OnionV2(_) | SocketAddress::OnionV3 { .. } => {
                // Onion addresses require TOR proxy
                let tor_config = tor_config.unwrap_or_default();
                let proxy_addr = tor_config.proxy_addr();
                let stream = Socks5Stream::connect(proxy_addr.as_str(), addr)
                    .await
                    .map_err(|e| Error::ProxyConnection(e.to_string()))?;
                LnStream::Tor(stream)
            }
            _ => {
                // Direct connection for non-onion addresses
                let sock_addrs: Vec<std::net::SocketAddr> = socket_addr
                    .to_socket_addrs()
                    .map_err(|_| Error::DnsError)?
                    .collect();

                let sock_addr = sock_addrs.first().ok_or(Error::DnsError)?;
                let socket = if sock_addr.is_ipv4() {
                    TcpSocket::new_v4()?
                } else {
                    TcpSocket::new_v6()?
                };
                LnStream::Direct(socket.connect(*sock_addr).await?)
            }
        };

        let ephemeral = SecretKey::new(&mut rand::thread_rng());

        let mut channel = PeerChannelEncryptor::new_outbound(their_pubkey, ephemeral);
        let act_one = channel.get_act_one(&secp_ctx);
        stream.write_all(&act_one).await?;

        let mut act_two = [0u8; ACT_TWO_SIZE];
        stream.read_exact(&mut act_two).await?;
        let act_three = channel.process_act_two(&secp_ctx, &act_two, &our_key)?;

        // Finalize the handshake by sending act3
        stream.write_all(&act_three).await?;

        Ok(Self {
            channel,
            stream,
            reconnect: ReconnectData {
                our_key,
                their_pubkey,
                addr: addr.to_string(),
            },
            frame: PartialFrame::default(),
        })
    }

    /// A socket over an already-connected stream whose handshake is done, for tests that play
    /// the peer themselves. Reconnecting it fails: its reconnect address goes nowhere.
    #[cfg(test)]
    pub(crate) fn from_finished(
        channel: PeerChannelEncryptor,
        stream: tokio::net::TcpStream,
    ) -> LNSocket {
        let our_key = SecretKey::from_slice(&[0x44; 32]).expect("valid key");
        let their_pubkey = PublicKey::from_secret_key(&Secp256k1::signing_only(), &our_key);
        Self {
            channel,
            stream: LnStream::Direct(stream),
            reconnect: ReconnectData {
                our_key,
                their_pubkey,
                addr: "127.0.0.1:1".to_string(),
            },
            frame: PartialFrame::default(),
        }
    }

    /// Connect as above and also perform a minimal `init` exchange.
    /// Fails with `Error::FirstMessageNotInit` if the peer's first message isn't `Init`.
    pub async fn connect_and_init(
        our_key: SecretKey,
        their_pubkey: PublicKey,
        addr: &str,
    ) -> Result<LNSocket, Error> {
        Self::connect_and_init_with_tor_config(our_key, their_pubkey, addr, None).await
    }

    /// Connect with optional TOR proxy configuration and perform a minimal `init` exchange.
    /// Fails with `Error::FirstMessageNotInit` if the peer's first message isn't `Init`.
    pub async fn connect_and_init_with_tor_config(
        our_key: SecretKey,
        their_pubkey: PublicKey,
        addr: &str,
        tor_config: Option<TorConfig>,
    ) -> Result<LNSocket, Error> {
        let mut lnsocket =
            LNSocket::connect_with_tor_config(our_key, their_pubkey, addr, tor_config).await?;
        lnsocket.perform_init().await?;
        Ok(lnsocket)
    }

    /// Build a brand-new socket using the stored reconnect inputs.
    pub async fn reconnect_fresh(&self) -> Result<LNSocket, Error> {
        LNSocket::connect_and_init(
            self.reconnect.our_key,
            self.reconnect.their_pubkey,
            &self.reconnect.addr,
        )
        .await
    }

    /// Completes the initial `init` message exchange.
    ///
    /// This must be called before issuing any other Lightning messages.
    /// Fails if the first incoming message isn’t `Init`.
    pub async fn perform_init(&mut self) -> Result<(), Error> {
        // first message should be init, if not, we fail
        if let Message::Init(init_msg) = self.read().await? {
            // send some bs
            self.write(&msgs::Init {
                features: vec![0; 5],
                global_features: vec![0; 2],
                remote_network_address: None,
                networks: init_msg.networks,
            })
            .await?;

            Ok(())
        } else {
            Err(Error::FirstMessageNotInit)
        }
    }

    pub async fn write<M: wire::Type + Writeable>(&mut self, m: &M) -> Result<(), io::Error> {
        let msg = self.channel.encrypt_message(m);
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<Message<()>, Error> {
        self.read_custom(|_type, _buf| Ok(None)).await
    }

    /// Read the next message, handing custom message types to `handler`.
    ///
    /// Cancel-safe: if the future is dropped before it completes, the part of the frame it had
    /// read stays on the socket and the next call picks up from there, so it can be raced
    /// against other work in `tokio::select!`.
    pub async fn read_custom<T>(
        &mut self,
        handler: impl FnOnce(u16, &mut Cursor<&[u8]>) -> Result<Option<T>, DecodeError>,
    ) -> Result<Message<T>, Error>
    where
        T: core::fmt::Debug,
    {
        let buf = self.read_frame().await?;
        let mut cursor = io::Cursor::new(&buf[..]);

        Ok(wire::read(&mut cursor, handler).map_err(|(de, _)| de)?)
    }

    /// Read and decrypt one whole frame, returning its plaintext body.
    ///
    /// Every await is in [`LNSocket::fill`], which is cancel-safe; the state changes between
    /// them happen without yielding, so a drop at any await loses nothing.
    async fn read_frame(&mut self) -> Result<Vec<u8>, Error> {
        if self.frame.body_len.is_none() {
            self.fill(18).await?;
            let hdr: [u8; 18] = self.frame.buf[..18].try_into().expect("filled 18 bytes");
            let size = self.channel.decrypt_length_header(&hdr)? as usize;
            self.frame.body_len = Some(size);
            self.frame.filled = 0;
        }

        let size = self.frame.body_len.expect("header decrypted above");
        self.fill(size + 16).await?;
        let mut buf = std::mem::take(&mut self.frame).buf;
        self.channel.decrypt_message(&mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    /// Read until the current stage of the frame holds `want` bytes.
    ///
    /// Cancel-safe: a single `read` either returns bytes or reads none, and the count is
    /// recorded before the next await.
    async fn fill(&mut self, want: usize) -> io::Result<()> {
        self.frame.buf.resize(want, 0);
        while self.frame.filled < want {
            let n = self
                .stream
                .read(&mut self.frame.buf[self.frame.filled..])
                .await?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.frame.filled += n;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CallOpts;
    use crate::ln::msgs;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_ping_pong() -> Result<(), Error> {
        let key = SecretKey::new(&mut rand::thread_rng());
        let their_key = PublicKey::from_str(
            "03f3c108ccd536b8526841f0a5c58212bb9e6584a1eb493080e7c1cc34f82dad71",
        )
        .unwrap();

        let mut lnsocket = LNSocket::connect_and_init(key, their_key, "ln.damus.io:9735").await?;

        //println!("got here");
        lnsocket
            .write(&msgs::Ping {
                ponglen: 4,
                byteslen: 8,
            })
            .await?;

        loop {
            if let Message::Pong(_) = lnsocket.read().await? {
                break;
            } else {
                // didn't get pong?
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_commando() -> Result<(), Error> {
        use crate::commando::CommandoClient;
        use bitcoin::secp256k1::{PublicKey, SecretKey, rand};
        use serde_json::json;
        use std::str::FromStr;

        let key = SecretKey::new(&mut rand::thread_rng());
        let pk_str = "03f3c108ccd536b8526841f0a5c58212bb9e6584a1eb493080e7c1cc34f82dad71";
        let their_key = PublicKey::from_str(pk_str).unwrap();

        // Connect as before
        let sock = LNSocket::connect_and_init(key, their_key, "ln.damus.io:9735").await?;

        // New API: spawn the client (it owns the socket + pump task)
        let commando = CommandoClient::spawn(
            sock,
            "hfYByx-RDwdBfAK-vOWeOCDJVYlvKSioVKU_y7jccZU9MjkmbWV0aG9kPWdldGluZm8=",
        );

        // New call signature: no socket arg, optional wait timeout
        let resp_fut = commando.call_with_opts(
            "getinfo",
            json!({}),
            CallOpts::default().filter(json!({"id": true})),
        );

        let bad_resp_fut = commando.call("invoice", json!({"msatoshi": "any"}));

        let resp = resp_fut.await?;

        assert_eq!(resp["id"].as_str().unwrap(), pk_str);

        // test filtering
        assert_eq!(resp.get("alias").is_none(), true);

        let bad_resp = bad_resp_fut.await.err().unwrap();
        if let Error::Rpc(rpc_err) = bad_resp {
            assert_eq!(rpc_err.code, 19537);
            assert_eq!(
                rpc_err.message,
                "Invalid rune: Not permitted: method is not equal to getinfo"
            );
        } else {
            assert_eq!(true, false);
        }

        Ok(())
    }
}
