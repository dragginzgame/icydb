//! Module: executor::pipeline::operators::reducer
//! Responsibility: reducer contracts plus aggregate reducer runner wiring.
//! Does not own: access-path resolution or planning-time aggregate eligibility.
//! Boundary: reusable reducer operators for execution-kernel orchestration.

mod aggregate;
mod contracts;
mod runner;

pub(in crate::db::executor) use contracts::{
    KernelReducer, ReducerControl, StreamInputMode, StreamItem,
};
