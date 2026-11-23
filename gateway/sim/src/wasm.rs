//! Browser build: the same engine behind a few C-style exports, so the
//! monitor page can run scenarios without a server.
//!
//! Text goes in through `gw_alloc` + `gw_load`; every call that produces
//! output leaves UTF-8 JSON in a buffer read with `gw_out_ptr`/`gw_out_len`.

use std::cell::RefCell;

use crate::report::Report;
use crate::scenario::Scenario;
use crate::sim::Sim;

thread_local! {
    static TUNER: RefCell<Option<crate::tune::Tuner>> = const { RefCell::new(None) };
    static SIM: RefCell<Option<Sim>> = const { RefCell::new(None) };
    static OUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn out(text: String) {
    OUT.with(|o| *o.borrow_mut() = text.into_bytes());
}

#[no_mangle]
pub extern "C" fn gw_alloc(len: usize) -> *mut u8 {
    let mut v = Vec::<u8>::with_capacity(len.max(1));
    let p = v.as_mut_ptr();
    std::mem::forget(v);
    p
}

/// Load a scenario (TOML or JSON) from memory handed out by `gw_alloc`,
/// which this call takes back. Returns 0, or 1 with the error as output.
///
/// # Safety
/// `ptr` must come from `gw_alloc(len)` and hold `len` initialised bytes.
#[no_mangle]
pub unsafe extern "C" fn gw_load(ptr: *mut u8, len: usize) -> i32 {
    let bytes = Vec::from_raw_parts(ptr, len, len.max(1));
    let text = String::from_utf8_lossy(&bytes);
    match Scenario::parse(&text).and_then(Sim::new) {
        Ok(sim) => {
            SIM.with(|s| *s.borrow_mut() = Some(sim));
            out(String::new());
            0
        }
        Err(e) => {
            out(e);
            1
        }
    }
}

/// Advance virtual time by `ms`, keeping the samples taken on the way for
/// the next `gw_take`. Returns 1 once the run is over, 0 before, -1 when no
/// scenario is loaded.
#[no_mangle]
pub extern "C" fn gw_step(ms: f64) -> i32 {
    SIM.with(|s| {
        let mut s = s.borrow_mut();
        let Some(sim) = s.as_mut() else {
            return -1;
        };
        if !sim.finished() {
            sim.advance(sim.now + ms.max(0.0));
        }
        i32::from(sim.finished())
    })
}

/// Output `{"now":s,"done":bool,"frames":[...]}` with the samples since the
/// last call (see `metrics::compact`). With `full` not 0 the last of them
/// carries the whole frame; the caller asks for that only as often as it
/// redraws, because a whole frame of a large system is big.
#[no_mangle]
pub extern "C" fn gw_take(full: i32) -> i32 {
    SIM.with(|s| {
        let mut s = s.borrow_mut();
        let Some(sim) = s.as_mut() else {
            out("no scenario loaded".into());
            return 1;
        };
        let frames = std::mem::take(&mut sim.metrics.frames);
        let last = frames.len().saturating_sub(1);
        let done = sim.finished();
        out(serde_json::json!({
            "now": sim.now / 1000.0,
            "done": done,
            "frames": crate::metrics::compact(&frames, |i| i == last && (full != 0 || done)),
        })
        .to_string());
        0
    })
}

/// Output the loaded scenario as JSON, with every `count` expanded and
/// every `headroom` turned into its cap: what the run is actually made of.
#[no_mangle]
pub extern "C" fn gw_scenario() -> i32 {
    SIM.with(|s| match s.borrow().as_ref() {
        Some(sim) => {
            out(serde_json::to_string(&sim.sc).unwrap_or_default());
            0
        }
        None => {
            out("no scenario loaded".into());
            1
        }
    })
}

/// Summary of the run so far (final once the run is done).
#[no_mangle]
pub extern "C" fn gw_report() -> i32 {
    SIM.with(|s| {
        let s = s.borrow();
        let Some(sim) = s.as_ref() else {
            out("no scenario loaded".into());
            return 1;
        };
        out(serde_json::to_string(&Report::from_sim(sim)).unwrap_or_default());
        0
    })
}

/// Run a scenario (handed over as in `gw_load`) to its end without keeping
/// it, and output `{"scores":[...],"idle":[...]}`. For comparing variants.
/// Returns 0, or 1 with the error as output.
///
/// # Safety
/// `ptr` must come from `gw_alloc(len)` and hold `len` initialised bytes.
#[no_mangle]
pub unsafe extern "C" fn gw_score(ptr: *mut u8, len: usize) -> i32 {
    let bytes = Vec::from_raw_parts(ptr, len, len.max(1));
    let text = String::from_utf8_lossy(&bytes);
    match Scenario::parse(&text).and_then(Sim::new) {
        Ok(mut sim) => {
            sim.run();
            out(serde_json::json!({
                "scores": crate::tune::scores(&sim),
                "idle": crate::tune::idle(&sim),
                "prefix": [sim.prefix_tokens, sim.prefix_tokens_saved],
            })
            .to_string());
            0
        }
        Err(e) => {
            out(e);
            1
        }
    }
}

/// Start a settings search on a scenario (TOML or JSON, handed over as in
/// `gw_load`) with `tries` tries of `seeds` runs each. Returns 0, or 1 with
/// the error as output.
///
/// # Safety
/// `ptr` must come from `gw_alloc(len)` and hold `len` initialised bytes.
#[no_mangle]
pub unsafe extern "C" fn gw_tune_begin(
    ptr: *mut u8,
    len: usize,
    tries: usize,
    seeds: usize,
) -> i32 {
    let bytes = Vec::from_raw_parts(ptr, len, len.max(1));
    let text = String::from_utf8_lossy(&bytes);
    match Scenario::parse(&text) {
        Ok(sc) => {
            TUNER.with(|t| *t.borrow_mut() = Some(crate::tune::Tuner::new(sc, tries, seeds)));
            out(String::new());
            0
        }
        Err(e) => {
            out(e);
            1
        }
    }
}

/// Run one more try of the search and output its state. Returns 1 once the
/// search is over, 0 before, -1 on an error (the output says which).
#[no_mangle]
pub extern "C" fn gw_tune_step() -> i32 {
    TUNER.with(|t| {
        let mut t = t.borrow_mut();
        let Some(tuner) = t.as_mut() else {
            out("no search started".into());
            return -1;
        };
        match tuner.step() {
            Ok(done) => {
                out(serde_json::to_string(&tuner.state()).unwrap_or_default());
                i32::from(done)
            }
            Err(e) => {
                out(e);
                -1
            }
        }
    })
}

/// Output the scenario with the settings of try `pick` written into it.
#[no_mangle]
pub extern "C" fn gw_tune_scenario(pick: usize) -> i32 {
    TUNER.with(
        |t| match t.borrow().as_ref().and_then(|t| t.scenario(pick)) {
            Some(sc) => {
                out(serde_json::to_string(&sc).unwrap_or_default());
                0
            }
            None => {
                out("no such try".into());
                1
            }
        },
    )
}

#[no_mangle]
pub extern "C" fn gw_out_ptr() -> *const u8 {
    OUT.with(|o| o.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn gw_out_len() -> usize {
    OUT.with(|o| o.borrow().len())
}
