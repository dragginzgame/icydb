//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: terminal-runtime boundary for cursorless structural load
//! row collection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes the terminal runtime surface while keeping the
//! row-collector implementation in one owner-local child.

mod row_collector;
