//! Module: db::executor::route::hints
//! Responsibility: route-owned bounded-fetch and scan-budget hint derivation.
//! Does not own: route capability derivation or dispatch execution.
//! Boundary: emits optional hints consumed by stream/runtime surfaces.

mod aggregate;
mod load;
