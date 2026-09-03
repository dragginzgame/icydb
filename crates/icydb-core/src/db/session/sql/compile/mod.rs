//! Module: db::session::sql::compile
//! Responsibility: semantic SQL statement compilation.
//! Does not own: SQL parsing, cache lookup, or execution.
//! Boundary: compiles parsed SQL statements into session-owned command artifacts.

mod semantic_compiler;
