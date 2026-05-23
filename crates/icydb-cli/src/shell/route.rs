//! Module: shell SQL routing.
//! Responsibility: classify shell SQL text as query or DDL before endpoint dispatch.
//! Does not own: ICP execution, response decoding, or SQL parsing semantics.
//! Boundary: exposes routing decisions to the shell runner and test-only shell wrappers.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlShellCallKind {
    Query,
    Ddl,
}

pub(super) fn sql_shell_call_kind(sql: &str) -> SqlShellCallKind {
    let normalized = sql
        .trim_start()
        .trim_end_matches(|ch: char| ch == ';' || ch.is_whitespace())
        .trim_start();
    let mut words = normalized.split_whitespace().map(str::to_ascii_uppercase);
    let first = words.next();
    let second = words.next();
    let third = words.next();
    if sql_shell_statement_is_ddl(first.as_deref(), second.as_deref(), third.as_deref()) {
        return SqlShellCallKind::Ddl;
    }

    SqlShellCallKind::Query
}

fn sql_shell_statement_is_ddl(
    first: Option<&str>,
    second: Option<&str>,
    third: Option<&str>,
) -> bool {
    matches!(
        (first, second, third),
        (Some("CREATE" | "DROP"), Some("INDEX"), _)
            | (Some("CREATE"), Some("UNIQUE"), Some("INDEX"))
            | (Some("ALTER"), Some("TABLE"), _)
    )
}
