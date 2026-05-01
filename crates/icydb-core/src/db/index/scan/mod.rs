//! Module: index::scan
//! Responsibility: raw-range index store traversal.
//! Does not own: cursor continuation, executor metrics, predicate execution, or row decoding.
//! Boundary: executor/query range readers wrap this layer with runtime policy.

mod raw;
#[cfg(test)]
mod tests;
