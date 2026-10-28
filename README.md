# lnsocket-rs

A Rust library for establishing connections to Lightning Network nodes. This library provides low-level primitives for establishing encrypted communication channels with Lightning Network nodes using the Noise_XK protocol as specified in [BOLT #8](https://github.com/lightning/bolts/blob/master/08-transport.md).

lnsocket also comes batteries included with Commando support, allowing you to call RPCs on core-lightning nodes remotely!

## Features

- [x] Establish encrypted connections to Lightning Network nodes with Noise_XK handshake protocol
- [x] Send and receive Lightning Network messages
- [x] Support for Commando CLN RPC messages
- [x] TOR connections for .onion addresses with automatic detection

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
lnsocket = "0.1.0"
```

## TOR Support

lnsocket-rs automatically detects and connects to `.onion` addresses via TOR, providing support for hidden services.

### Default Configuration

- **Default SOCKS proxy**: `127.0.0.1:9050` (standard TOR proxy address)
- The library automatically detects `.onion` addresses and routes through TOR

### Basic Usage

```rust
use bitcoin::secp256k1::{SecretKey, PublicKey, rand};
use lnsocket::LNSocket;

async fn connect_onion() -> Result<(), lnsocket::Error> {
    let key = SecretKey::new(&mut rand::thread_rng());
    let pk = PublicKey::from_str("03...")?.unwrap();

    // Onion addresses automatically use TOR (127.0.0.1:9050)
    let mut sock = LNSocket::connect_and_init(key, pk, "node.onion:9735").await?;

    // Use the socket normally - TOR is handled transparently
    Ok(())
}
```

### Custom TOR Proxy Configuration

```rust
use lnsocket::{LNSocket, TorConfig};

async fn connect_custom_proxy() -> Result<(), lnsocket::Error> {
    let key = SecretKey::new(&mut rand::thread_rng());
    let pk = PublicKey::from_str("03...")?.unwrap();

    // Custom TOR proxy (e.g., different port or remote proxy)
    let tor_config = TorConfig::new("127.0.0.1".to_string(), 9150);
    let mut sock = LNSocket::connect_and_init_with_tor_config(
        key, pk, "node.onion:9735", Some(tor_config)
    ).await?;

    Ok(())
}
```

## Commando over LNSocket

This crate includes a small [Commando][commando] client that runs **over the same encrypted Lightning transport**.

```rust
use bitcoin::secp256k1::{SecretKey, PublicKey, rand};
use lnsocket::{LNSocket, CommandoClient};
use serde_json::json;
use lnsocket::commando::CallOpts;

async fn commando_rpc_demo(pk: PublicKey, rune: &str) -> Result<(), lnsocket::Error> {
    let key = SecretKey::new(&mut rand::thread_rng());
    let sock = LNSocket::connect_and_init(key, pk, "ln.example.com:9735").await?;
    let client = CommandoClient::spawn(sock, rune);

    // Inherit client defaults (30s timeout, auto-reconnect with backoff,
    // and retry up to 3 times). Override per call if needed:
    let res = client.call("getinfo", json!({})).await?;
    println!("{}", res);

    let opts = CallOpts::new().timeout(std::time::Duration::from_secs(5)).retry(5);
    let channels = client.call_with_opts("listchannels", json!({}), &opts).await?;
    Ok(())
}

```

## lnrpc

`lnrpc` is a small command line tool for calling Core Lightning RPCs over Commando:

```sh
cargo install lnsocket --bin lnrpc

lnrpc <nodeid> <host[:port]> <rune> getinfo
lnrpc <nodeid> <host[:port]> <rune> listinvoices '{"label":"foo"}'
```

Pass several `method [params]` pairs to call them concurrently over one connection;
results are printed in argument order:

```sh
lnrpc <nodeid> <host> <rune> getinfo listfunds listpeerchannels '{"id":"02..."}'
```

The port defaults to 9735 and `LNRPC_TIMEOUT` sets the per-call timeout in milliseconds
(default 30000). Calls are never retried, so a dropped connection won't resend a `pay`.

## Status

This library is experimental and under active development. APIs may change significantly between versions.

## License

This library contains code derived from LDK, which is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](http://www.apache.org/licenses/LICENSE-2.0))
- MIT License ([LICENSE-MIT](http://opensource.org/licenses/MIT))

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

[commando]: https://docs.corelightning.org/reference/commando
