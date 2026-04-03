use crate::{
    db::{DbSession, StorageReport},
    error::Error,
    traits::CanisterKind,
};

#[cfg(feature = "sql")]
use crate::{
    db::{EntityAuthority, sql::SqlQueryResult},
    error::{ErrorKind, QueryErrorKind},
};

///
/// Execute one generated storage snapshot request through the hidden facade.
///
/// This helper keeps the generated metrics endpoint on the default snapshot
/// path so canister exports do not retain alias-remapping diagnostics helpers
/// they never use.
///
pub fn execute_generated_storage_report<C: CanisterKind>(
    session: &DbSession<C>,
) -> Result<StorageReport, Error> {
    Ok(session.inner.storage_report_default()?)
}

///
/// Execute one generated SQL query surface request through the hidden facade.
///
/// This helper keeps the generated build output close to a thin ABI shim while
/// the core session owns parse, route, authority, and metadata/query
/// dispatch. The facade only maps the final result into `SqlQueryResult` and
/// preserves the public EXPLAIN error rewrite contract.
///
#[cfg(feature = "sql")]
pub fn execute_generated_sql_query<C: CanisterKind>(
    session: &DbSession<C>,
    sql: &str,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    // Phase 1: execute the generated query surface entirely through the core
    // dispatch owner and retain the explain-hint context for public rewrite.
    let attempt = session
        .inner
        .execute_generated_query_surface_sql(sql, authorities);
    let entity_name = attempt.entity_name().to_string();
    let explain_order_field = attempt.explain_order_field();

    // Phase 2: map success onto the public SQL payload surface or preserve the
    // generated EXPLAIN unordered-pagination hint for the public error shape.
    match attempt.into_result() {
        Ok(result) => Ok(DbSession::<C>::map_sql_dispatch_result(result, entity_name)),
        Err(err) => {
            let facade = Error::from(err);

            if let Some(order_field) = explain_order_field {
                Err(explain_surface_error(sql, order_field, facade))
            } else {
                Err(facade)
            }
        }
    }
}

// Preserve the public generated-EXPLAIN unordered-pagination guidance while
// keeping the main generated route family in core.
#[cfg(feature = "sql")]
fn explain_surface_error(sql: &str, order_field: &str, err: Error) -> Error {
    if !matches!(
        err.kind(),
        ErrorKind::Query(QueryErrorKind::UnorderedPagination)
    ) {
        return err;
    }

    let target_sql = explain_target_sql(sql);
    let suggestion = explain_order_hint_sql(target_sql, order_field);
    let message = format!(
        "Cannot EXPLAIN this SQL statement.\n\nReason:\nLIMIT or OFFSET without ORDER BY is non-deterministic.\n\nSQL:\n{target_sql}\n\nHow to fix:\nAdd ORDER BY for a stable total order, for example:\n{suggestion}",
    );

    Error::new(
        ErrorKind::Query(QueryErrorKind::UnorderedPagination),
        err.origin(),
        message,
    )
}

// Strip the EXPLAIN prefix so the public hint can show the underlying query.
#[cfg(feature = "sql")]
fn explain_target_sql(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    if let Some(next) = consume_keyword(rest, "EXPLAIN") {
        rest = next;
        if let Some(next) = consume_keyword(rest, "EXECUTION") {
            rest = next;
        } else if let Some(next) = consume_keyword(rest, "JSON") {
            rest = next;
        }
    }

    rest.trim_start()
}

// Synthesize one deterministic EXPLAIN fix-up query for the public hint.
#[cfg(feature = "sql")]
fn explain_order_hint_sql(target_sql: &str, order_field: &str) -> String {
    let trimmed = target_sql.trim().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();

    if let Some(index) = upper.find(" LIMIT ") {
        return format!(
            "EXPLAIN {} ORDER BY {order_field} ASC{}",
            &trimmed[..index],
            &trimmed[index..]
        );
    } else if let Some(index) = upper.find(" OFFSET ") {
        return format!(
            "EXPLAIN {} ORDER BY {order_field} ASC{}",
            &trimmed[..index],
            &trimmed[index..]
        );
    }

    format!("EXPLAIN {trimmed} ORDER BY {order_field} ASC")
}

// Consume one standalone SQL keyword while leaving longer identifiers intact.
#[cfg(feature = "sql")]
fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = input.strip_prefix(keyword)?;

    if rest
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }

    Some(rest.trim_start())
}
