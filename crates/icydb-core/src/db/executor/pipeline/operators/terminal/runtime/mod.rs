//! Module: executor::pipeline::operators::terminal::runtime
//! Responsibility: cursorless structural row collection.
//! Does not own: route selection or session response shaping.
//! Boundary: keeps row-collector mechanics local to the executor.

mod row_collector;
