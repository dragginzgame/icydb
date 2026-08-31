use icydb::{
    db::{
        DbSession, DynamicQuery, LiveQueryPageOutput, PrimaryKeyValue, TypedEntityAdapter,
        TypedWrite,
        query::{
            CollectionOperator, CompareOperator, FieldCompareOperator, FilterExpr, FilterValue,
            JunctionOperator, Query, SetOperator, StateOperator, count,
        },
    },
    traits::CanisterKind,
};

#[allow(dead_code)]
fn dynamic_queries_compile_without_sql<C>(db: &DbSession<C>)
where
    C: CanisterKind,
{
    let grouped_filter = FilterExpr::Compare {
        operator: CompareOperator::Eq,
        field: "age".to_string(),
        value: FilterValue::from(42_u64),
    };
    let operator_surface = (
        JunctionOperator::And,
        FieldCompareOperator::Eq,
        SetOperator::In,
        CollectionOperator::Contains,
        StateOperator::IsNull,
    );
    std::hint::black_box(operator_surface);
    let request = DynamicQuery::new("app::User")
        .filter(grouped_filter)
        .select(["name", "age"])
        .limit(25);
    let _ = db.execute_live_page(&request, None);
    let _ = db.execute_live_page_with_attribution(&request, None);
    let _ = db.execute_trusted_live_page(&request, None);
    let mut public_continuation = None;
    let _ = db
        .advance_live_page(&request, public_continuation.as_deref())
        .map(|step| (step.is_exhausted(), step.commit(&mut public_continuation)));
    let _ = db
        .advance_live_page(&request, public_continuation.as_deref())
        .map(icydb::db::LivePageStep::into_page);
    let mut trusted_continuation = None;
    let _ = db
        .advance_trusted_live_page(&request, trusted_continuation.as_deref())
        .map(|step| (step.is_exhausted(), step.commit(&mut trusted_continuation)));
    let _ = db.execute_exhaustive_page(&request, None, None);
    let _ = db.execute_trusted_exhaustive_page(&request, None, None);
    let _ = db.capture_read_set_revision_proof(&["app::User"]);
    let exact = DynamicQuery::new("app::User");
    let _ = db.execute_exact_count(&exact);
    if let Ok(job_id) = icydb::db::ResumableJobId::try_from_bytes([1; 32]) {
        let _ = db.acknowledge_resumable_job(job_id, 1);
    }

    let grouped = DynamicQuery::new("app::User")
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25);
    let _ = db.execute_public_dynamic_grouped_query(&grouped);
    let _ = db.execute_trusted_dynamic_grouped_query(&grouped);
}

#[allow(dead_code)]
fn typed_exhaustive_queries_compile_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query.limit(25).execute_exhaustive_page(None, None);
}

#[allow(dead_code)]
fn typed_attributed_queries_compile_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query
        .filter(FilterExpr::eq("owner_id", 1_u64))
        .filter(FilterExpr::eq("slot", 2_u64))
        .limit(25)
        .execute_live_page_with_attribution(None);
}

#[allow(dead_code)]
fn typed_exact_count_compiles_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query.execute_exact_count();
}

#[allow(dead_code)]
fn typed_grouped_queries_compile_without_sql<C, E>(query: Query<'_, C, E>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let _ = query
        .group_by("age")
        .aggregate(count())
        .grouped_limits(100, 64 * 1024)
        .limit(25)
        .execute_grouped();
}

#[allow(dead_code)]
fn typed_dynamic_live_page_adapter<C, E>(
    db: &DbSession<C>,
    request: &DynamicQuery,
    continuation: Option<&str>,
    trusted: bool,
) where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(db) else {
        return;
    };
    let cursor = db.prepare_live_page_cursor(binding, request.clone());
    let prepared = if trusted {
        cursor.execute_trusted_page(continuation)
    } else {
        cursor.execute_page(continuation)
    };
    let Ok(prepared) = prepared else {
        return;
    };
    let mut rows = Vec::with_capacity(prepared.rows.len());
    for row in prepared.rows {
        let Ok(row) = E::decode_row(cursor.binding(), row) else {
            return;
        };
        rows.push(row);
    }
    std::hint::black_box((rows, prepared.continuation, prepared.work));
}

#[allow(dead_code)]
fn prepared_dynamic_row_batch_compiles<C, E>(db: &DbSession<C>, result: LiveQueryPageOutput)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(db) else {
        return;
    };
    let LiveQueryPageOutput {
        entity,
        columns,
        rows,
        row_count: _,
        continuation,
        work,
    } = result;
    let Ok(rows) = db.prepare_typed_output_rows(&binding, entity, columns, rows) else {
        return;
    };
    std::hint::black_box((rows, continuation, work));
}

#[allow(dead_code)]
fn prepared_exact_key_batch_compiles<C, E>(db: &DbSession<C>, keys: &[PrimaryKeyValue])
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(db) else {
        return;
    };
    let Ok(prepared) = db.execute_public_prepared_exact_key_batch(&binding, keys) else {
        return;
    };
    let distinct_rows = prepared
        .distinct_rows
        .into_iter()
        .map(|row| row.map(|row| E::decode_row(&binding, row)))
        .collect::<Vec<_>>();
    std::hint::black_box((distinct_rows, prepared.positions));
}

#[allow(dead_code)]
fn prepared_same_entity_write_batch_compiles<C, E>(db: &DbSession<C>, writes: Vec<TypedWrite>)
where
    C: CanisterKind,
    E: TypedEntityAdapter,
{
    let Ok(binding) = E::typed_binding(db) else {
        return;
    };
    let Ok(rows) = db.execute_trusted_typed_write_batch_rows(&binding, writes) else {
        return;
    };
    let decoded = rows
        .into_iter()
        .map(|row| E::decode_row(&binding, row))
        .collect::<Vec<_>>();
    std::hint::black_box(decoded);
}

#[test]
fn public_query_facade_compile_contract() {}
