use crate::{guard, Cancel, Image};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

pub(super) const CODE: &str = concat!(
    include_str!("../../guest/agent_checkpoint.py"),
    "\n",
    include_str!("../../guest/checklist_agent.py")
);
pub(super) const CAP: usize = 8192;
const PREFIX: &str = "PYBOX_AGENT_V1 ";

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Frame {
    Paused {
        run_id: String,
        guest_nonce: String,
        memory: Value,
    },
    Completed {
        run_id: String,
        guest_nonce: String,
        checkpoint_id: String,
        result: Value,
    },
}

pub(super) fn frame(output: &[u8]) -> Result<Option<Frame>> {
    let mut matching = output
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(|line| line.strip_suffix(b"\n")?.strip_prefix(PREFIX.as_bytes()));
    let Some(line) = matching.next() else {
        return Ok(None);
    };
    ensure!(matching.next().is_none(), "multiple agent protocol frames");
    serde_json::from_slice(line)
        .context("invalid agent protocol frame")
        .map(Some)
}

pub(super) fn check_memory(memory: &Value, binding: &str) -> Result<()> {
    ensure!(
        memory["schema"] == "pybox.langgraph-memory.v1"
            && memory["binding"] == binding
            && memory["payload"]
                .as_str()
                .is_some_and(|data| !data.is_empty())
            && serde_json::to_vec(memory)?.len() <= 4096,
        "invalid or incompatible agent memory"
    );
    Ok(())
}

pub(super) fn execute(
    image: &Image,
    request: &Value,
    phase: &str,
    cancel: &Cancel,
    events: bool,
    flush: Option<(guard::Observer, &AtomicBool)>,
) -> Result<guard::Receipt> {
    let mut args = image.execution_args(None)?;
    let json = serde_json::to_string(request)?;
    // Keep the encoded input well below the host command-line limits.
    ensure!(
        json.len() <= 6144,
        "agent request exceeds the guest transport limit"
    );
    args.extend([
        "--env".into(),
        format!("PYBOX_AGENT_REQUEST={json}").into(),
        "--exec".into(),
        CODE.into(),
    ]);
    let (observer, stop) = match flush {
        Some((observer, stop)) => (Some(observer), Some(stop)),
        None => (None, None),
    };
    let result = guard::run_controlled(
        &image.binary,
        &args,
        &image.root,
        guard::Profile::Execution,
        Duration::from_secs(15),
        CAP,
        cancel,
        events,
        phase,
        observer,
        stop,
    );
    if let Err(error) = &result {
        if error.downcast_ref::<guard::CleanupError>().is_some() {
            std::fs::write(
                image.root.join("state/unclean.json"),
                serde_json::to_vec(&json!({"phase": phase, "error": error.to_string()}))?,
            )
            .context("record agent cleanup uncertainty")?;
        }
    }
    result
}

pub(super) fn clean_receipt(receipt: &guard::Receipt, status: &str) -> Result<()> {
    if receipt.status != status
        || !receipt.resources_reclaimed
        || receipt.output_truncated
        || receipt.error.is_some()
        || receipt
            .policy_receipt
            .as_ref()
            .and_then(|policy| policy["applied"].as_bool())
            != Some(true)
    {
        bail!(
            "agent VM did not finish {status} with verified cleanup (status {})",
            receipt.status
        );
    }
    Ok(())
}
