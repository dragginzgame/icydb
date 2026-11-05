//! Test-only helpers for the parent module.

pub(crate) fn sql_parse_count_for_tests() -> usize {
    SQL_PARSE_INVOCATIONS.with(std::cell::Cell::get)
}

thread_local! {
    pub(super) static SQL_PARSE_INVOCATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
