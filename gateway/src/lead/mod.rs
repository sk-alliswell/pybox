//! The three areas where the gateway aims to lead: deliverable-based ROI,
//! fair scheduling per agent run, and token savings backed by measurement.
//! They share a run identity, a prefix fingerprint and a cost ledger.

pub mod adaptive;
pub mod cachectl;
pub mod cachemodel;
pub mod diagnose;
pub mod engine;
pub mod eval;
pub mod experiment;
pub mod fair;
pub mod fingerprint;
pub mod http;
pub mod json;
pub mod ledger;
pub mod prefix;
pub mod runs;
pub mod sim;
pub mod trim;
