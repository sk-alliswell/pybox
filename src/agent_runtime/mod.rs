//! Single-host lifecycle for registered, checkpoint-capable agents.
mod protocol;
mod store;

use crate::{guard, platform, Cancel, Image, Lease};
use anyhow::{bail, ensure, Context, Result};
use protocol::Frame;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const MAX_INPUT_BYTES: u64 = 2048;
const SCHEMA: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Agent {
    Checklist,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChecklistInput {
    items: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    schema: u32,
    run_id: String,
    agent: Agent,
    source_sha256: String,
    image: Value,
    input_sha256: String,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    binding: String,
    guest_nonce: String,
    memory: Value,
}

#[derive(Serialize, Deserialize)]
struct VmProof {
    pid: u32,
    guest_nonce: String,
    resources_reclaimed: bool,
}

#[derive(Serialize, Deserialize)]
struct Ready {
    checkpoint_id: String,
    spawn_vm: VmProof,
}

#[derive(Serialize, Deserialize)]
struct Claim {
    checkpoint_id: String,
}

#[derive(Serialize, Deserialize)]
struct Completion {
    checkpoint_id: String,
    resume_vm: VmProof,
    result: Value,
}

fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn source_digest() -> String {
    format!("{:x}", Sha256::digest(protocol::CODE.as_bytes()))
}

fn random_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("random source: {error}"))?;
    Ok(format!("{:032x}", u128::from_be_bytes(bytes)))
}

fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn event(events: bool, value: Value) {
    if events {
        eprintln!("{value}");
    }
}

fn agent_dir(root: &Path, run_id: &str) -> Result<PathBuf> {
    ensure!(valid_hex(run_id, 32), "invalid agent run ID");
    Ok(root.join("state/agents").join(run_id))
}

fn manifest(directory: &Path, run_id: &str) -> Result<Manifest> {
    let record: Manifest = store::read(directory, "run.json")?.context("missing agent manifest")?;
    ensure!(
        record.schema == SCHEMA && record.run_id == run_id,
        "invalid agent manifest"
    );
    Ok(record)
}

fn agent_image(root: &Path, cancel: &Cancel) -> Result<Image> {
    let image = Image::open(root, cancel)?;
    let capability = platform::capability();
    ensure!(
        capability["available"] == true,
        "host virtualization unavailable; no fallback: {capability}"
    );
    ensure!(
        !image.root.join("state/unclean.json").try_exists()?,
        "unhealthy: unresolved cleanup receipt"
    );
    Ok(image)
}

fn checkpoint(directory: &Path, manifest: &Manifest, ready: &Ready) -> Result<Checkpoint> {
    let record: Checkpoint =
        store::read(directory, "memory.json")?.context("missing agent memory")?;
    ensure!(
        record.binding == digest(manifest)?
            && digest(&record)? == ready.checkpoint_id
            && record.guest_nonce == ready.spawn_vm.guest_nonce
            && ready.spawn_vm.resources_reclaimed,
        "agent checkpoint integrity mismatch"
    );
    protocol::check_memory(&record.memory, &record.binding)?;
    Ok(record)
}

struct Flushed {
    checkpoint_id: String,
    guest_nonce: String,
}

#[derive(Default)]
struct Flush {
    output: Vec<u8>,
    result: Option<Result<Flushed>>,
}

/// Launch a registered agent and return its run ID after the checkpoint is published
/// and the originating VM is verified gone.
pub fn spawn(
    root: &Path,
    agent: Agent,
    input: Value,
    cancel: &Cancel,
    events: bool,
) -> Result<Value> {
    let parsed: ChecklistInput =
        serde_json::from_value(input.clone()).context("invalid checklist input")?;
    ensure!(
        !parsed.items.is_empty()
            && parsed.items.len() <= 12
            && parsed
                .items
                .iter()
                .all(|item| !item.trim().is_empty() && item.len() <= 128)
            && serde_json::to_vec(&input)?.len() as u64 <= MAX_INPUT_BYTES,
        "checklist requires 1 to 12 non-empty items of up to 128 bytes"
    );

    let image = agent_image(root, cancel)?;
    let state = image.root.join("state");
    store::private_directory(&state)?;
    let _lease = Lease::acquire(&state.join("leases"))?;
    store::private_directory(&state.join("agents"))?;
    let run_id = random_id()?;
    let directory = agent_dir(&image.root, &run_id)?;
    store::private_directory(&directory)?;
    let _run_lock = store::lock(&directory, true)?;
    let manifest = Manifest {
        schema: SCHEMA,
        run_id: run_id.clone(),
        agent,
        image: image.identity.clone(),
        source_sha256: source_digest(),
        input_sha256: digest(&input)?,
    };
    store::publish(&directory, "run.json", &manifest)?;
    let binding = digest(&manifest)?;
    event(
        events,
        json!({"event": "agent_run_created", "run_id": run_id}),
    );

    let state = Arc::new(Mutex::new(Flush::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let observer: guard::Observer = {
        let (state, stop) = (state.clone(), stop.clone());
        let (directory, binding, run_id) = (directory.clone(), binding.clone(), run_id.clone());
        Arc::new(move |chunk| {
            let mut state = state.lock().expect("flush observer state");
            if state.result.is_some() || state.output.len() + chunk.len() > protocol::CAP {
                return;
            }
            state.output.extend_from_slice(chunk);
            let frame = match protocol::frame(&state.output) {
                Ok(Some(frame)) => frame,
                Ok(None) => return,
                Err(error) => {
                    state.result = Some(Err(error));
                    return;
                }
            };
            state.result = Some((|| {
                let Frame::Paused {
                    run_id: returned_id,
                    guest_nonce,
                    memory,
                } = frame
                else {
                    bail!("expected an agent pause frame");
                };
                ensure!(
                    returned_id == run_id && valid_hex(&guest_nonce, 32),
                    "invalid spawn identity"
                );
                protocol::check_memory(&memory, &binding)?;
                let checkpoint = Checkpoint {
                    binding: binding.clone(),
                    guest_nonce: guest_nonce.clone(),
                    memory,
                };
                let checkpoint_id = digest(&checkpoint)?;
                store::publish(&directory, "memory.json", &checkpoint)
                    .context("durable agent memory flush failed")?;
                event(
                    events,
                    json!({"event":"memory_flushed", "run_id":run_id, "checkpoint_id":checkpoint_id}),
                );
                stop.store(true, Ordering::Release);
                Ok(Flushed {
                    checkpoint_id,
                    guest_nonce,
                })
            })());
        })
    };
    let request = json!({"phase":"spawn","run_id":run_id,"binding":binding,"input":input});
    let receipt = protocol::execute(
        &image,
        &request,
        "agent_spawn",
        cancel,
        events,
        Some((observer, &stop)),
    )?;
    event(
        events,
        json!({"event":"native_reaped","phase":"agent_spawn","pid":receipt.process_id}),
    );
    let flushed = state
        .lock()
        .expect("flush observer state")
        .result
        .take()
        .with_context(|| {
            format!(
                "agent did not flush a checkpoint (status {})",
                receipt.status
            )
        })??;
    protocol::clean_receipt(&receipt, "paused")?;
    ensure!(
        matches!(
            protocol::frame(receipt.stdout.as_bytes())?,
            Some(Frame::Paused { .. })
        ),
        "missing pause frame"
    );
    let ready = Ready {
        checkpoint_id: flushed.checkpoint_id,
        spawn_vm: VmProof {
            pid: receipt.process_id,
            guest_nonce: flushed.guest_nonce,
            resources_reclaimed: receipt.resources_reclaimed,
        },
    };
    store::publish(&directory, "ready.json", &ready)?;
    Ok(
        json!({"run_id":run_id,"status":"paused","checkpoint_id":ready.checkpoint_id,"spawn_vm":ready.spawn_vm}),
    )
}

/// Restore a claimed checkpoint in a fresh VM. A completed claim returns its
/// stored result on repeated requests. An uncertain claim never launches again.
pub fn resume(
    root: &Path,
    run_id: &str,
    expected: &str,
    cancel: &Cancel,
    events: bool,
) -> Result<Value> {
    ensure!(valid_hex(expected, 64), "invalid checkpoint ID");
    let root = root.canonicalize()?;
    let directory = agent_dir(&root, run_id)?;
    let _run_lock = store::lock(&directory, false)?;
    let manifest = manifest(&directory, run_id)?;
    let ready: Ready = store::read(&directory, "ready.json")?
        .context("agent checkpoint is not ready to resume")?;
    ensure!(
        expected == ready.checkpoint_id,
        "checkpoint ID does not match the paused agent"
    );
    let checkpoint = checkpoint(&directory, &manifest, &ready)?;
    if let Some(claim) = store::read::<Claim>(&directory, "resume.json")? {
        ensure!(claim.checkpoint_id == expected, "invalid resume claim");
        let result: Completion = store::read(&directory, "complete.json")?
            .context("resume already claimed; outcome uncertain, automatic relaunch refused")?;
        ensure!(result.checkpoint_id == expected, "invalid resume result");
        return Ok(completed_view(run_id, &ready, &result, true));
    }

    let image = agent_image(&root, cancel)?;
    ensure!(
        manifest.image == image.identity && manifest.source_sha256 == source_digest(),
        "agent checkpoint uses an incompatible image or agent implementation"
    );
    let _lease = Lease::acquire(&image.root.join("state/leases"))?;
    ensure!(
        !cancel.cancelled(),
        "cancelled before claiming the agent checkpoint"
    );
    let request = json!({"phase":"resume","run_id":run_id,"binding":checkpoint.binding,
                         "checkpoint_id":expected,"memory":checkpoint.memory});

    // Persist the single-use claim before crossing the launch boundary.
    store::publish(
        &directory,
        "resume.json",
        &Claim {
            checkpoint_id: expected.to_owned(),
        },
    )?;
    event(
        events,
        json!({"event":"resume_claimed","run_id":run_id,"checkpoint_id":expected}),
    );
    let receipt = protocol::execute(&image, &request, "agent_resume", cancel, events, None)?;
    event(
        events,
        json!({"event":"native_reaped","phase":"agent_resume","pid":receipt.process_id}),
    );
    protocol::clean_receipt(&receipt, "completed")?;
    let Some(Frame::Completed {
        run_id: returned_id,
        guest_nonce,
        checkpoint_id,
        result,
    }) = protocol::frame(receipt.stdout.as_bytes())?
    else {
        bail!("expected an agent completion frame");
    };
    ensure!(
        returned_id == run_id
            && checkpoint_id == expected
            && valid_hex(&guest_nonce, 32)
            && guest_nonce != ready.spawn_vm.guest_nonce,
        "invalid resume identity"
    );
    let complete = Completion {
        checkpoint_id,
        resume_vm: VmProof {
            pid: receipt.process_id,
            guest_nonce,
            resources_reclaimed: receipt.resources_reclaimed,
        },
        result,
    };
    store::publish(&directory, "complete.json", &complete)?;
    Ok(completed_view(run_id, &ready, &complete, false))
}

fn completed_view(run_id: &str, ready: &Ready, complete: &Completion, replayed: bool) -> Value {
    json!({"run_id":run_id,"status":"completed","checkpoint_id":ready.checkpoint_id,
           "spawn_vm":ready.spawn_vm,"resume_vm":complete.resume_vm,"result":complete.result,"replayed":replayed})
}

/// Inspect committed lifecycle state without launching a VM.
pub fn status(root: &Path, run_id: &str) -> Result<Value> {
    let directory = agent_dir(&root.canonicalize()?, run_id)?;
    let _run_lock = store::lock(&directory, false)?;
    let manifest = manifest(&directory, run_id)?;
    let Some(ready) = store::read::<Ready>(&directory, "ready.json")? else {
        return Ok(json!({"run_id":run_id,"status":"spawn_interrupted"}));
    };
    checkpoint(&directory, &manifest, &ready)?;
    if let Some(claim) = store::read::<Claim>(&directory, "resume.json")? {
        ensure!(
            claim.checkpoint_id == ready.checkpoint_id,
            "invalid resume claim"
        );
        if let Some(complete) = store::read::<Completion>(&directory, "complete.json")? {
            ensure!(
                complete.checkpoint_id == ready.checkpoint_id,
                "invalid resume result"
            );
            return Ok(completed_view(run_id, &ready, &complete, true));
        }
        return Ok(
            json!({"run_id":run_id,"status":"resume_outcome_uncertain","checkpoint_id":ready.checkpoint_id}),
        );
    }
    Ok(
        json!({"run_id":run_id,"status":"paused","checkpoint_id":ready.checkpoint_id,"spawn_vm":ready.spawn_vm}),
    )
}
