//! `lnrpc`: call Core Lightning RPCs over Commando from the command line.
//!
//! A port of the `lnrpc` tool from the C lnsocket library, plus named nodes
//! kept in a TOML config so the node id, host and rune aren't retyped:
//!
//! ```text
//! lnrpc config add <name> <nodeid> <host[:port]> <rune>
//! lnrpc [-n <name>] <method> [params (json)] [<method> [params] ...]
//! lnrpc <nodeid> <host[:port]> <rune> <method> [params] [<method> [params] ...]
//! ```
//!
//! Several `method [params]` pairs may be given. They are sent concurrently
//! over one connection and their results are printed in argument order. An
//! argument starting with `{` or `[` is the params of the method before it.
//!
//! The config lives at `-c <path>`, `$LNRPC_CONFIG`,
//! `$XDG_CONFIG_HOME/lnrpc/config.toml` or `~/.config/lnrpc/config.toml`,
//! first found wins. The port defaults to 9735. `LNRPC_TIMEOUT` sets the call
//! timeout in milliseconds (default 30000).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::secp256k1::{PublicKey, SecretKey, rand};
use lnsocket::commando::{CommandoConfig, RetryPolicy};
use lnsocket::{CommandoClient, Error, LNSocket};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const USAGE: &str = "\
usage: lnrpc [-c <config>] [-n <node>] <method> [params (json)] [<method> [params] ...]
       lnrpc <nodeid> <host[:port]> <rune> <method> [params] [<method> [params] ...]
       lnrpc [-c <config>] config <add|rm|default|list|path> ...

  config add <name> <nodeid> <host[:port]> <rune>   save a node (first one becomes default)
  config rm <name>                                  forget a node
  config default <name>                             use this node when -n is not given
  config list                                       show saved nodes (runes are not printed)
  config path                                       print the config file location

multiple methods are called concurrently over one connection.
currently supports commando for clightning, but potentially more rpc types in the future!";

#[derive(Debug, Default, Serialize, Deserialize)]
struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default: Option<String>,
    #[serde(default)]
    nodes: BTreeMap<String, Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Node {
    nodeid: String,
    host: String,
    rune: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(args).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Returns whether every call succeeded.
async fn run(mut args: Vec<String>) -> Result<bool, String> {
    let mut config_path = None;
    let mut node_name = None;
    while let Some(flag) = args.first().map(String::as_str) {
        let slot = match flag {
            "-c" | "--config" => &mut config_path,
            "-n" | "--node" => &mut node_name,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(true);
            }
            _ => break,
        };
        if args.len() < 2 {
            return Err(format!("{flag} needs a value"));
        }
        *slot = Some(args.remove(1));
        args.remove(0);
    }

    if args.is_empty() {
        eprintln!("{USAGE}");
        return Ok(false);
    }

    // An explicit node id first means the C lnrpc form; nothing else is read.
    if PublicKey::from_str(&args[0]).is_ok() {
        if node_name.is_some() {
            return Err("-n can't be used with an explicit nodeid".into());
        }
        if args.len() < 4 {
            eprintln!("{USAGE}");
            return Ok(false);
        }
        let node = Node {
            nodeid: args[0].clone(),
            host: args[1].clone(),
            rune: args[2].clone(),
        };
        return call(&node, parse_calls(&args[3..])?).await;
    }

    let path = match config_path {
        Some(p) => PathBuf::from(p),
        None => default_config_path()?,
    };

    if args[0] == "config" {
        config_cmd(&path, &args[1..])?;
        return Ok(true);
    }

    let config = load_config(&path)?;
    let name = node_name.or(config.default.clone()).ok_or_else(|| {
        format!(
            "no node given and no default in {}; add one with `lnrpc config add`",
            path.display()
        )
    })?;
    let node = config
        .nodes
        .get(&name)
        .ok_or_else(|| format!("no node named '{name}' in {}", path.display()))?;

    call(node, parse_calls(&args)?).await
}

fn default_config_path() -> Result<PathBuf, String> {
    if let Some(p) = std::env::var_os("LNRPC_CONFIG") {
        return Ok(PathBuf::from(p));
    }
    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x),
        _ => PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(".config"),
    };
    Ok(base.join("lnrpc").join("config.toml"))
}

fn load_config(path: &Path) -> Result<Config, String> {
    match std::fs::read_to_string(path) {
        Ok(s) => toml::from_str(&s).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(format!("reading {}: {e}", path.display())),
    }
}

/// Writes the config readable by its owner only, since it holds runes.
fn save_config(path: &Path, config: &Config) -> Result<(), String> {
    use std::io::Write;

    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    }
    let text = toml::to_string_pretty(config).map_err(|e| e.to_string())?;

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    opts.open(path)
        .and_then(|mut f| f.write_all(text.as_bytes()))
        .map_err(|e| format!("writing {}: {e}", path.display()))
}

fn config_cmd(path: &Path, args: &[String]) -> Result<(), String> {
    let usage = || format!("bad config command\n\n{USAGE}");
    let sub = args.first().ok_or_else(usage)?.as_str();
    let mut config = load_config(path)?;

    match (sub, &args[1..]) {
        ("path", []) => println!("{}", path.display()),
        ("list", []) => {
            for (name, node) in &config.nodes {
                let mark = if config.default.as_ref() == Some(name) {
                    "*"
                } else {
                    " "
                };
                println!("{mark} {name}  {}@{}", node.nodeid, node.host);
            }
        }
        ("add", [name, nodeid, host, rune]) => {
            PublicKey::from_str(nodeid).map_err(|e| format!("invalid nodeid '{nodeid}': {e}"))?;
            config.nodes.insert(
                name.clone(),
                Node {
                    nodeid: nodeid.clone(),
                    host: host.clone(),
                    rune: rune.clone(),
                },
            );
            if config.default.is_none() {
                config.default = Some(name.clone());
            }
            save_config(path, &config)?;
        }
        ("rm", [name]) => {
            config
                .nodes
                .remove(name)
                .ok_or_else(|| format!("no node named '{name}'"))?;
            if config.default.as_ref() == Some(name) {
                config.default = None;
            }
            save_config(path, &config)?;
        }
        ("default", [name]) => {
            if !config.nodes.contains_key(name) {
                return Err(format!("no node named '{name}'"));
            }
            config.default = Some(name.clone());
            save_config(path, &config)?;
        }
        _ => return Err(usage()),
    }
    Ok(())
}

/// Split `method [params] method [params] ...` into calls.
fn parse_calls(args: &[String]) -> Result<Vec<(String, Value)>, String> {
    if args.is_empty() {
        return Err(format!("no method given\n\n{USAGE}"));
    }
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
async fn call(node: &Node, calls: Vec<(String, Value)>) -> Result<bool, String> {
    let nodeid = &node.nodeid;
    let their_pubkey =
        PublicKey::from_str(nodeid).map_err(|e| format!("invalid nodeid '{nodeid}': {e}"))?;

    let addr = if node.host.contains(':') {
        node.host.clone()
    } else {
        format!("{}:9735", node.host)
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
        node.rune.as_str(),
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
