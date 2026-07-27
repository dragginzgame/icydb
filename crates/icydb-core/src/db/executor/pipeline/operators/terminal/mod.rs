//! Module: executor::pipeline::operators::terminal
//! Responsibility: structural row-collector materialization.
//! Does not own: aggregate reducers or route planning.
//! Boundary: owns the cursorless row-collector short path.

mod runtime;
