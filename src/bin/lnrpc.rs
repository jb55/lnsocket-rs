//! `lnrpc`: call Core Lightning RPCs over Commando from the command line.
//!
//! A port of the `lnrpc` tool from the C lnsocket library:
//!
//! ```text
//! lnrpc <nodeid> <host[:port]> <rune> <method> [params (json)] [<method> [params] ...]
//! ```
//!
//! Several `method [params]` pairs may be given. They are sent concurrently
//! over one connection and their results are printed in argument order. An
//! argument starting with `{` or `[` is the params of the method before it.
//!
//! The port defaults to 9735. `LNRPC_TIMEOUT` sets the call timeout in
//! milliseconds (default 30000).

use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::secp256k1::{PublicKey, SecretKey, rand};
use lnsocket::commando::{CommandoConfig, RetryPolicy};
use lnsocket::{CommandoClient, Error, LNSocket};
use serde_json::Value;

const USAGE: &str = "usage: lnrpc <nodeid> <host[:port]> <rune> <method> [params (json)] [<method> [params] ...]\n\n\
multiple methods are called concurrently over one connection.\n\
currently supports commando for clightning, but potentially more rpc types in the future!";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 4 {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    }

    match run(&args).await {
        Ok(ok) if ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Split `method [params] method [params] ...` into calls.
fn parse_calls(args: &[String]) -> Result<Vec<(String, Value)>, String> {
    let mut calls = Vec::new();
    let mut args = args.iter().peekable();
    while let Some(method) = args.next() {
        let params = match args.next_if(|a| a.starts_with('{') || a.starts_with('[')) {
            Some(p) => serde_json::from_str(p)
                .map_err(|e| format!("invalid params json for {method}: {e}"))?,
            None => Value::Object(Default::default()),
        };
        calls.push((method.clone(), params));
    }
    Ok(calls)
}

/// Returns whether every call succeeded.
async fn run(args: &[String]) -> Result<bool, String> {
    let (nodeid, host, rune) = (&args[0], &args[1], &args[2]);

    let their_pubkey =
        PublicKey::from_str(nodeid).map_err(|e| format!("invalid nodeid '{nodeid}': {e}"))?;

    let calls = parse_calls(&args[3..])?;

    let addr = if host.contains(':') {
        host.clone()
    } else {
        format!("{host}:9735")
    };

    let timeout_ms = match std::env::var("LNRPC_TIMEOUT") {
        Ok(t) => t
            .parse::<u64>()
            .map_err(|_| format!("invalid LNRPC_TIMEOUT '{t}'"))?,
        Err(_) => 30_000,
    };

    let key = SecretKey::new(&mut rand::thread_rng());
    let sock = LNSocket::connect_and_init(key, their_pubkey, &addr)
        .await
        .map_err(|e| format!("connecting to {addr}: {e}"))?;

    // A one-shot CLI shouldn't silently resend a command (eg. `pay`) after a
    // broken connection, so turn off reconnects and retries.
    let config = CommandoConfig::new()
        .timeout(Some(Duration::from_millis(timeout_ms)))
        .no_reconnect()
        .retry_policy(RetryPolicy::Never);
    let commando = Arc::new(CommandoClient::spawn_with_config(
        sock,
        rune.as_str(),
        config,
    ));

    // Fire every call before awaiting any; the pump demuxes the replies.
    let handles: Vec<_> = calls
        .into_iter()
        .map(|(method, params)| {
            let commando = commando.clone();
            let task_method = method.clone();
            let handle = tokio::spawn(async move { commando.call(task_method, params).await });
            (method, handle)
        })
        .collect();

    let mut all_ok = true;
    for (method, handle) in handles {
        match handle.await {
            Ok(Ok(result)) => println!("{}", serde_json::to_string_pretty(&result).unwrap()),
            Ok(Err(Error::Rpc(rpc))) => {
                eprintln!("error: {method}: {} (code {})", rpc.message, rpc.code);
                all_ok = false;
            }
            Ok(Err(e)) => {
                eprintln!("error: {method}: {e}");
                all_ok = false;
            }
            Err(e) => {
                eprintln!("error: {method}: task failed: {e}");
                all_ok = false;
            }
        }
    }

    Ok(all_ok)
}
