use anyhow::{bail, Context, Result};
use python_sandbox_controller::{agent_runtime, Cancel};
use serde_json::Value;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub(super) const USAGE: &str = "\
pybox agent spawn checklist --input FILE [--root BUNDLE] [--events]
pybox agent resume RUN_ID CHECKPOINT_ID [--root BUNDLE] [--events]
pybox agent status RUN_ID [--root BUNDLE]";

enum Action {
    Spawn,
    Resume {
        run_id: String,
        checkpoint_id: String,
    },
    Status {
        run_id: String,
    },
}

fn required(value: Option<&OsString>, label: &str) -> Result<String> {
    Ok(value
        .and_then(|v| v.to_str())
        .with_context(|| format!("expected {label}"))?
        .to_owned())
}

pub(super) fn run(args: &[OsString], cancel: &Cancel) -> Result<Value> {
    let mut args = args.iter();
    let action = match args.next().and_then(|arg| arg.to_str()) {
        Some("spawn") if args.next().is_some_and(|value| value == "checklist") => Action::Spawn,
        Some("resume") => Action::Resume {
            run_id: required(args.next(), "run ID")?,
            checkpoint_id: required(args.next(), "checkpoint ID")?,
        },
        Some("status") => Action::Status {
            run_id: required(args.next(), "run ID")?,
        },
        _ => bail!("{USAGE}"),
    };

    let mut root = None;
    let mut input = None;
    let mut events = false;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--root") if root.is_none() => {
                root = Some(PathBuf::from(
                    args.next().context("--root needs a bundle path")?,
                ));
            }
            Some("--input") if input.is_none() && matches!(action, Action::Spawn) => {
                input = Some(args.next().context("--input needs a JSON file")?);
            }
            Some("--events") if !events && !matches!(action, Action::Status { .. }) => {
                events = true
            }
            _ => bail!("unexpected agent argument {:?}", arg),
        }
    }
    let root = match root {
        Some(root) => root,
        None => std::env::current_exe()?
            .parent()
            .context("executable directory")?
            .to_owned(),
    };
    let _gate = crate::host_gate::acquire_standalone()?;
    match action {
        Action::Spawn => {
            let input = read_input(input.context("agent spawn requires --input FILE")?)?;
            agent_runtime::spawn(
                &root,
                agent_runtime::Agent::Checklist,
                input,
                cancel,
                events,
            )
        }
        Action::Resume {
            run_id,
            checkpoint_id,
        } => agent_runtime::resume(&root, &run_id, &checkpoint_id, cancel, events),
        Action::Status { run_id } => agent_runtime::status(&root, &run_id),
    }
}

fn read_input(path: &OsStr) -> Result<Value> {
    let mut input = Vec::new();
    File::open(Path::new(path))?
        .take(2049)
        .read_to_end(&mut input)?;
    anyhow::ensure!(input.len() <= 2048, "agent input exceeds 2048 bytes");
    serde_json::from_slice(&input).context("invalid agent input JSON")
}
