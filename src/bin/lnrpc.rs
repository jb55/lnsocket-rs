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
//! With no method, `lnrpc` connects once and reads calls from stdin, one line
//! at a time, using the same rule (`listinvoices {"label":"foo bar"}`). A
//! terminal gets a prompt and history; piped lines are run without one.
//!
//! The config lives at `-c <path>`, `$LNRPC_CONFIG`,
//! `$XDG_CONFIG_HOME/lnrpc/config.toml` or `~/.config/lnrpc/config.toml`,
//! first found wins. The port defaults to 9735. `LNRPC_TIMEOUT` sets the call
//! timeout in milliseconds (default 30000). REPL history is kept in
//! `$XDG_STATE_HOME/lnrpc/history` or `~/.local/state/lnrpc/history`.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::secp256k1::{PublicKey, SecretKey, rand};
use lnsocket::commando::{CommandoConfig, RetryPolicy};
use lnsocket::{CommandoClient, Error, LNSocket};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

const USAGE: &str = "\
usage: lnrpc [-c <config>] [-n <node>] [<method> [params (json)] [<method> [params] ...]]
       lnrpc <nodeid> <host[:port]> <rune> [<method> [params] [<method> [params] ...]]
       lnrpc [-c <config>] config <add|rm|default|list|path> ...

  config add <name> <nodeid> <host[:port]> <rune>   save a node (first one becomes default)
  config rm <name>                                  forget a node
  config default <name>                             use this node when -n is not given
  config list                                       show saved nodes (runes are not printed)
  config path                                       print the config file location

multiple methods are called concurrently over one connection. with no method,
read `method [params]` lines from stdin over one connection (exit or ctrl-d quits).
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

    // An explicit node id first means the C lnrpc form; nothing else is read.
    if args.first().is_some_and(|a| PublicKey::from_str(a).is_ok()) {
        if node_name.is_some() {
            return Err("-n can't be used with an explicit nodeid".into());
        }
        if args.len() < 3 {
            eprintln!("{USAGE}");
            return Ok(false);
        }
        let node = Node {
            nodeid: args[0].clone(),
            host: args[1].clone(),
            rune: args[2].clone(),
        };
        if args.len() == 3 {
            return repl(&node, "lnrpc").await;
        }
        return call(&node, parse_calls(&args[3..])?).await;
    }

    let path = match config_path {
        Some(p) => PathBuf::from(p),
        None => default_config_path()?,
    };

    if args.first().is_some_and(|a| a == "config") {
        config_cmd(&path, &args[1..])?;
        return Ok(true);
    }

    let config = load_config(&path)?;
    // A bare `lnrpc` with nothing configured is asking how to use it.
    if args.is_empty() && node_name.is_none() && config.nodes.is_empty() {
        eprintln!("{USAGE}");
        return Ok(false);
    }
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

    if args.is_empty() {
        return repl(node, &name).await;
    }
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

fn call_timeout() -> Result<Duration, String> {
    let timeout_ms = match std::env::var("LNRPC_TIMEOUT") {
        Ok(t) => t
            .parse::<u64>()
            .map_err(|_| format!("invalid LNRPC_TIMEOUT '{t}'"))?,
        Err(_) => 30_000,
    };
    Ok(Duration::from_millis(timeout_ms))
}

async fn connect(node: &Node, config: CommandoConfig) -> Result<Arc<CommandoClient>, String> {
    let nodeid = &node.nodeid;
    let their_pubkey =
        PublicKey::from_str(nodeid).map_err(|e| format!("invalid nodeid '{nodeid}': {e}"))?;

    let addr = if node.host.contains(':') {
        node.host.clone()
    } else {
        format!("{}:9735", node.host)
    };

    let key = SecretKey::new(&mut rand::thread_rng());
    let sock = LNSocket::connect_and_init(key, their_pubkey, &addr)
        .await
        .map_err(|e| format!("connecting to {addr}: {e}"))?;

    Ok(Arc::new(CommandoClient::spawn_with_config(
        sock,
        node.rune.as_str(),
        config,
    )))
}

/// Returns whether every call succeeded.
async fn call(node: &Node, calls: Vec<(String, Value)>) -> Result<bool, String> {
    // A one-shot CLI shouldn't silently resend a command (eg. `pay`) after a
    // broken connection, so turn off reconnects and retries.
    let config = CommandoConfig::new()
        .timeout(Some(call_timeout()?))
        .no_reconnect()
        .retry_policy(RetryPolicy::Never);
    let commando = connect(node, config).await?;
    Ok(run_calls(&commando, calls).await)
}

/// Fire every call before awaiting any, then print the results in order.
/// Returns whether every call succeeded.
async fn run_calls(commando: &Arc<CommandoClient>, calls: Vec<(String, Value)>) -> bool {
    // The pump demuxes the replies.
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
            Ok(Ok(result)) => {
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
                continue;
            }
            Ok(Err(Error::Rpc(rpc))) => {
                eprintln!("error: {method}: {} (code {})", rpc.message, rpc.code);
            }
            Ok(Err(Error::Io(std::io::ErrorKind::BrokenPipe))) => {
                eprintln!(
                    "error: {method}: connection lost before a reply; \
                     not resent, it may or may not have run on the node"
                );
            }
            Ok(Err(e)) => eprintln!("error: {method}: {e}"),
            Err(e) => eprintln!("error: {method}: task failed: {e}"),
        }
        all_ok = false;
    }
    all_ok
}

/// Read `method [params] ...` lines and call them over one connection.
///
/// Returns whether every line succeeded when stdin is piped; a terminal
/// session always exits successfully.
async fn repl(node: &Node, name: &str) -> Result<bool, String> {
    let interactive = std::io::stdin().is_terminal();

    // Unlike the one-shot CLI, a long-lived session should ride out a dropped
    // socket, so reconnects stay on. Retries stay off: on a broken pipe the
    // pump fails every in-flight `RetryPolicy::Never` call with BrokenPipe
    // instead of resending it, so a `pay` can't run twice behind our back.
    let config = CommandoConfig::new()
        .timeout(Some(call_timeout()?))
        .retry_policy(RetryPolicy::Never);

    // Connect before the first prompt so a bad node fails fast.
    let mut commando = connect(node, config.clone()).await?;
    let mut input = Input::new(interactive, format!("{name}> "));
    let mut all_ok = true;

    while let Some(line) = input.next_line().await? {
        if line.trim_start().starts_with('#') {
            continue;
        }
        let words = match split_line(&line) {
            Ok(words) => words,
            Err(e) => {
                eprintln!("error: {e}");
                all_ok = false;
                continue;
            }
        };
        match words.first().map(String::as_str) {
            None => continue,
            Some("exit" | "quit") if words.len() == 1 => break,
            Some(_) => {}
        }
        let calls = parse_calls(&words).expect("words is not empty");

        // The pump stops if it gives up reconnecting, so start over with a
        // fresh connection rather than fail every line from here on.
        if commando.is_closed() {
            match connect(node, config.clone()).await {
                Ok(c) => commando = c,
                Err(e) => {
                    eprintln!("error: {e}");
                    all_ok = false;
                    continue;
                }
            }
        }

        all_ok &= if interactive {
            tokio::select! {
                ok = run_calls(&commando, calls) => ok,
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("interrupted; the call may still complete on the node");
                    false
                }
            }
        } else {
            run_calls(&commando, calls).await
        };
    }

    input.save_history();
    Ok(all_ok || interactive)
}

/// Split a REPL line into the words `parse_calls` takes, as if each had been
/// its own argument. A word starting with `{` or `[` runs to the end of that
/// JSON value, so params may contain spaces.
fn split_line(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut rest = line.trim_start();
    while !rest.is_empty() {
        let len = if rest.starts_with(['{', '[']) {
            let mut values =
                serde_json::Deserializer::from_str(rest).into_iter::<serde::de::IgnoredAny>();
            if let Some(Err(e)) = values.next() {
                return Err(format!("invalid params json: {e}"));
            }
            let len = values.byte_offset();
            if len < rest.len() && !rest[len..].starts_with(char::is_whitespace) {
                return Err(format!(
                    "expected a space after params json: {}",
                    &rest[..len]
                ));
            }
            len
        } else {
            rest.find(char::is_whitespace).unwrap_or(rest.len())
        };
        words.push(rest[..len].to_string());
        rest = rest[len..].trim_start();
    }
    Ok(words)
}

/// REPL lines from a terminal (with editing and history) or a pipe.
enum Input {
    Tty {
        /// Moved into a blocking task while a line is read, so the pump keeps
        /// answering pings and noticing a dropped socket meanwhile.
        editor: Option<DefaultEditor>,
        prompt: String,
        history: Option<PathBuf>,
    },
    Pipe(tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>),
}

impl Input {
    fn new(interactive: bool, prompt: String) -> Self {
        if !interactive {
            return Input::Pipe(tokio::io::BufReader::new(tokio::io::stdin()).lines());
        }
        let editor = match DefaultEditor::new() {
            Ok(mut editor) => {
                let history = history_path();
                if let Some(path) = &history {
                    let _ = editor.load_history(path);
                }
                return Input::Tty {
                    editor: Some(editor),
                    prompt,
                    history,
                };
            }
            Err(e) => e,
        };
        eprintln!("warning: no line editing: {editor}");
        Input::Pipe(tokio::io::BufReader::new(tokio::io::stdin()).lines())
    }

    /// The next line, or `None` at end of input.
    async fn next_line(&mut self) -> Result<Option<String>, String> {
        match self {
            Input::Pipe(lines) => lines.next_line().await.map_err(|e| e.to_string()),
            Input::Tty { editor, prompt, .. } => loop {
                let mut ed = editor.take().expect("editor is put back after each line");
                let p = prompt.clone();
                let (ed, res) = tokio::task::spawn_blocking(move || {
                    let res = ed.readline(&p);
                    (ed, res)
                })
                .await
                .map_err(|e| e.to_string())?;
                let ed = editor.insert(ed);
                match res {
                    Ok(line) => {
                        if !line.trim().is_empty() {
                            let _ = ed.add_history_entry(line.as_str());
                        }
                        return Ok(Some(line));
                    }
                    // ctrl-c at the prompt drops the line, like a shell.
                    Err(ReadlineError::Interrupted) => continue,
                    Err(ReadlineError::Eof) => return Ok(None),
                    Err(e) => return Err(e.to_string()),
                }
            },
        }
    }

    fn save_history(&mut self) {
        let Input::Tty {
            editor: Some(editor),
            history: Some(path),
            ..
        } = self
        else {
            return;
        };
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Err(e) = editor.save_history(path) {
            eprintln!("warning: saving history to {}: {e}", path.display());
        }
    }
}

fn history_path() -> Option<PathBuf> {
    let base = match std::env::var_os("XDG_STATE_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x),
        _ => PathBuf::from(std::env::var_os("HOME")?).join(".local/state"),
    };
    Some(base.join("lnrpc").join("history"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<String> {
        split_line(line).unwrap()
    }

    #[test]
    fn split_line_plain_words() {
        assert_eq!(words("  getinfo   listfunds "), ["getinfo", "listfunds"]);
        assert!(words("   ").is_empty());
    }

    #[test]
    fn split_line_json_with_spaces() {
        assert_eq!(
            words(r#"listinvoices {"label": "foo bar"} getinfo"#),
            ["listinvoices", r#"{"label": "foo bar"}"#, "getinfo"]
        );
        assert_eq!(
            words(r#"decode [ "lnbc1 x", {"a": [1, 2]} ]"#),
            ["decode", r#"[ "lnbc1 x", {"a": [1, 2]} ]"#]
        );
        // A brace inside a string doesn't end the value early.
        assert_eq!(
            words(r#"invoice {"description":"} ]"}"#),
            ["invoice", r#"{"description":"} ]"}"#]
        );
    }

    #[test]
    fn split_line_matches_parse_calls() {
        let calls = parse_calls(&words(r#"listinvoices {"label":"a b"} getinfo"#)).unwrap();
        assert_eq!(calls[0].0, "listinvoices");
        assert_eq!(calls[0].1, serde_json::json!({"label": "a b"}));
        assert_eq!(calls[1].0, "getinfo");
        assert_eq!(calls[1].1, serde_json::json!({}));
    }

    #[test]
    fn split_line_rejects_bad_json() {
        assert!(split_line(r#"listinvoices {"label":"#).is_err());
        assert!(split_line(r#"listinvoices {"label":foo}"#).is_err());
        assert!(split_line(r#"listinvoices {"label":"a"}getinfo"#).is_err());
    }
}
