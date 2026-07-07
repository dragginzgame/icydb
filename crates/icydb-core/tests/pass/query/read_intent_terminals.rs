use icydb_core::{
    db::{
        AdminBatchRequest, FluentLoadQuery, PageRequest, PagedLoadExecution, PersistedRow,
        QueryError, ReadIntentKind,
    },
    traits::EntityValue,
};

fn page_request_terminals_compile<E>(
    query: FluentLoadQuery<'_, E>,
) -> Result<PagedLoadExecution<E>, QueryError>
where
    E: PersistedRow + EntityValue,
{
    query.execute_paged(PageRequest::first(10))
}

fn page_request_builder_compiles<E>(query: FluentLoadQuery<'_, E>) -> Result<(), QueryError>
where
    E: PersistedRow + EntityValue,
{
    let _ = query.page(PageRequest::new())?;
    Ok(())
}

fn admin_batch_first_compiles<E>(
    query: FluentLoadQuery<'_, E>,
) -> Result<PagedLoadExecution<E>, QueryError>
where
    E: PersistedRow + EntityValue,
{
    query
        .trusted_read_unchecked()
        .admin_batch(AdminBatchRequest::new())
}

fn admin_batch_next_compiles<E>(
    query: FluentLoadQuery<'_, E>,
) -> Result<PagedLoadExecution<E>, QueryError>
where
    E: PersistedRow + EntityValue,
{
    query
        .trusted_read_unchecked()
        .admin_batch(AdminBatchRequest::next("opaque-cursor"))
}

fn semantic_read_intent_terminals_compile<E>(
    query: &FluentLoadQuery<'_, E>,
) -> Result<(), QueryError>
where
    E: PersistedRow + EntityValue,
{
    let _: bool = query.exists()?;
    let _: Vec<E> = query.collect_complete()?;
    let _: u32 = query.count_exact()?;
    let _ = query.min_exact()?;
    let _ = query.min_exact_by("amount")?;
    let _ = query.max_exact()?;
    let _ = query.max_exact_by("amount")?;
    let _ = query.sum_exact("amount")?;
    let _ = query.avg_exact("amount")?;
    Ok(())
}

fn read_intent_kind_export_compiles() {
    let _: ReadIntentKind = ReadIntentKind::ExactAggregate;
}

fn main() {}
