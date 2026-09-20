//! Complete nested catalogue rows through the ordinary typed-query owners.

use crate::{
    typed_adapter_fixture_error, typed_fixture_invariant_error, typed_operation_fixture_error,
};
use candid::CandidType;
use ic_cdk::{api::performance_counter, update};
use icydb::{
    db::{
        DynamicQuery, StructuralPatch, TypedRowAdapter, WriteCell, query::asc,
        with_request_execution,
    },
    traits::EntitySource,
    value::{InputValue, OutputValue, PublicValue},
};
use icydb_testing_test_sql_fixtures::sql::{SqlTestCatalogItem, SqlTestCollectionProbe};

#[derive(CandidType)]
pub(crate) struct CatalogWorkloadRow {
    id: u64,
    key: String,
    name: String,
    description: String,
    placement: Option<CatalogWorkloadPlacement>,
    capacity: u64,
}

#[derive(CandidType)]
struct CatalogWorkloadPlacement {
    shape: Vec<u8>,
    points: Vec<(u64, u64)>,
    asset: String,
    enabled: bool,
}

#[derive(CandidType)]
pub(crate) struct CatalogWorkloadSample {
    result: Result<Vec<CatalogWorkloadRow>, icydb::Error>,
    total_instructions: u64,
    binding_instructions: u64,
    page_instructions: u64,
    adapter_instructions: u64,
    pages: u32,
}

#[derive(CandidType)]
pub(crate) struct CatalogLabel {
    id: u64,
    key: String,
    name: String,
}

#[derive(CandidType)]
pub(crate) struct CatalogLabelSample {
    result: Result<Vec<CatalogLabel>, icydb::Error>,
    instructions: u64,
    pages: u32,
}

// Selected values are consumed without rendering, coercion or default fields.
fn decode_label(values: Vec<OutputValue>) -> Result<CatalogLabel, icydb::Error> {
    let [id, key, name]: [OutputValue; 3] = values
        .try_into()
        .map_err(|_| typed_fixture_invariant_error())?;
    let (PublicValue::Nat64(id), PublicValue::Text(key), PublicValue::Text(name)) =
        (id.into_public(), key.into_public(), name.into_public())
    else {
        return Err(typed_fixture_invariant_error());
    };
    Ok(CatalogLabel { id, key, name })
}

#[update]
fn measure_catalog_labels(selected: bool) -> CatalogLabelSample {
    let mut sample = CatalogLabelSample {
        result: Err(typed_fixture_invariant_error()),
        instructions: 0,
        pages: 0,
    };
    sample.result = with_request_execution(|| {
        let session = icydb::db!()?;
        let start = performance_counter(1);
        let mut labels = Vec::new();
        let mut continuation = None;
        for _ in 0..4 {
            let (rows, next) = if selected {
                let columns = [
                    SqlTestCatalogItem::ID.as_str(),
                    SqlTestCatalogItem::KEY.as_str(),
                    SqlTestCatalogItem::NAME.as_str(),
                ];
                let request = DynamicQuery::new(SqlTestCatalogItem::ENTITY)
                    .select(columns)
                    .order_by(asc(SqlTestCatalogItem::KEY))
                    .limit(257);
                let page = session.execute_live_page(&request, continuation.as_deref())?;
                if page.columns != columns {
                    return Err(typed_fixture_invariant_error());
                }
                let rows = page
                    .rows
                    .into_iter()
                    .map(decode_label)
                    .collect::<Result<_, _>>()?;
                (rows, page.continuation)
            } else {
                let page = session
                    .query::<SqlTestCatalogItem>()
                    .map_err(typed_operation_fixture_error)?
                    .order_by(asc(SqlTestCatalogItem::KEY))
                    .limit(257)
                    .execute_live_page(continuation.as_deref())
                    .map_err(typed_operation_fixture_error)?;
                let rows = page
                    .rows
                    .into_iter()
                    .map(|row| CatalogLabel {
                        id: row.id,
                        key: row.key,
                        name: row.name,
                    })
                    .collect::<Vec<_>>();
                (rows, page.continuation)
            };
            sample.pages += 1;
            labels.extend(rows);
            if labels.len() > 128 {
                return Err(typed_fixture_invariant_error());
            }
            // Conversion completes before adopting either path's continuation.
            continuation = next;
            if continuation.is_none() {
                break;
            }
        }
        if continuation.is_some() {
            return Err(typed_fixture_invariant_error());
        }
        sample.instructions = performance_counter(1) - start;
        Ok(labels)
    });
    sample
}

fn record(fields: Vec<(&str, InputValue)>) -> InputValue {
    InputValue::map(
        fields
            .into_iter()
            .map(|(name, value)| (InputValue::text(name.into()), value))
            .collect(),
    )
}

#[update]
fn seed_catalog_workload(start: u32, count: u32) -> Result<(), icydb::Error> {
    with_request_execution(|| {
        if count != 16 || !start.is_multiple_of(16) || start > 112 {
            return Err(typed_fixture_invariant_error());
        }
        let session = icydb::db!()?;
        // Keep writes bounded and outside the measured read messages.
        for id in u64::from(start)..u64::from(start + count) {
            let placement = if id % 3 == 0 {
                InputValue::null()
            } else {
                record(vec![
                    (
                        "shape",
                        InputValue::blob(vec![
                            u8::try_from(id)
                                .map_err(|_| typed_fixture_invariant_error())?;
                            768
                        ]),
                    ),
                    (
                        "points",
                        InputValue::list(
                            (0..4)
                                .map(|x| {
                                    record(vec![
                                        ("x", InputValue::nat64(x)),
                                        ("y", InputValue::nat64(id + x)),
                                    ])
                                })
                                .collect(),
                        ),
                    ),
                    ("asset", InputValue::text(format!("asset-{id}"))),
                    ("enabled", InputValue::boolean(id % 2 == 0)),
                ])
            };
            let patch = StructuralPatch::new()
                .field("id", WriteCell::Value(InputValue::nat64(id)))
                .field(
                    "key",
                    WriteCell::Value(InputValue::text(format!("item-{id:04}"))),
                )
                .field(
                    "name",
                    WriteCell::Value(InputValue::text(format!("Item {id}"))),
                )
                .field(
                    "description",
                    WriteCell::Value(InputValue::text("catalogue description ".repeat(8))),
                )
                .field("placement", WriteCell::Value(placement))
                .field("capacity", WriteCell::Value(InputValue::nat64(id + 1)));
            session.execute_trusted_structural_insert_batch("SqlTestCatalogItem", vec![patch])?;
        }
        let binding =
            SqlTestCatalogItem::typed_binding(&session).map_err(typed_operation_fixture_error)?;
        let equivalent =
            SqlTestCatalogItem::typed_binding(&session).map_err(typed_operation_fixture_error)?;
        let different = SqlTestCollectionProbe::typed_binding(&session)
            .map_err(typed_operation_fixture_error)?;
        let cursor = session.prepare_live_page_cursor(
            binding.clone(),
            DynamicQuery::new(SqlTestCatalogItem::ENTITY)
                .order_by(asc(SqlTestCatalogItem::KEY))
                .limit(2),
        );
        let page = cursor
            .execute_page(None)
            .map_err(typed_operation_fixture_error)?;
        if page.rows.len() != 2 || equivalent != binding {
            return Err(typed_fixture_invariant_error());
        }
        // Independent equivalent bindings remain valid; a different binding
        // must reject before consuming any row field. Neither check is measured.
        for (expected_id, (selected, mut row)) in
            [binding, equivalent].iter().zip(page.rows).enumerate()
        {
            if !matches!(
                different.take_row_value("id", &mut row),
                Err(icydb::db::TypedAdapterError::StaleBinding)
            ) {
                return Err(typed_fixture_invariant_error());
            }
            let decoded = SqlTestCatalogItem::decode_row(selected, row)
                .map_err(typed_adapter_fixture_error)?;
            if decoded.id
                != u64::try_from(expected_id).map_err(|_| typed_fixture_invariant_error())?
            {
                return Err(typed_fixture_invariant_error());
            }
        }
        Ok(())
    })
}

#[update]
fn measure_catalog_workload(staged: bool) -> CatalogWorkloadSample {
    let mut sample = CatalogWorkloadSample {
        result: Err(typed_fixture_invariant_error()),
        total_instructions: 0,
        binding_instructions: 0,
        page_instructions: 0,
        adapter_instructions: 0,
        pages: 0,
    };
    sample.result = with_request_execution(|| {
        let session = icydb::db!()?;
        let start = performance_counter(1);
        let mut rows = Vec::new();
        let mut continuation = None;
        for _ in 0..4 {
            let page_start = performance_counter(1);
            let (page_rows, next) = if staged {
                // Same owners as Query::execute_live_page, with counters at its
                // existing boundaries; this is not a separate executor.
                let binding = SqlTestCatalogItem::typed_binding(&session)
                    .map_err(typed_operation_fixture_error)?;
                let cursor = session.prepare_live_page_cursor(
                    binding,
                    DynamicQuery::new(SqlTestCatalogItem::ENTITY)
                        .order_by(asc(SqlTestCatalogItem::KEY))
                        .limit(257),
                );
                let bound = performance_counter(1);
                let page = cursor
                    .execute_page(continuation.as_deref())
                    .map_err(typed_operation_fixture_error)?;
                let executed = performance_counter(1);
                let rows = page
                    .rows
                    .map(|row| {
                        SqlTestCatalogItem::decode_row(cursor.binding(), row)
                            .map_err(typed_adapter_fixture_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let adapted = performance_counter(1);
                sample.binding_instructions += bound - page_start;
                sample.page_instructions += executed - bound;
                sample.adapter_instructions += adapted - executed;
                (rows, page.continuation)
            } else {
                let page = session
                    .query::<SqlTestCatalogItem>()
                    .map_err(typed_operation_fixture_error)?
                    .order_by(asc(SqlTestCatalogItem::KEY))
                    .limit(257)
                    .execute_live_page(continuation.as_deref())
                    .map_err(typed_operation_fixture_error)?;
                (page.rows, page.continuation)
            };
            sample.pages += 1;
            rows.extend(page_rows);
            continuation = next;
            if rows.len() > 128 {
                return Err(typed_fixture_invariant_error());
            }
            if continuation.is_none() {
                break;
            }
        }
        if continuation.is_some() {
            return Err(typed_fixture_invariant_error());
        }
        sample.total_instructions = performance_counter(1) - start;
        // Transport shaping is outside the query interval. Every returned
        // field is independently checked by the host, including nested bytes.
        Ok(rows
            .into_iter()
            .map(|row| CatalogWorkloadRow {
                id: row.id,
                key: row.key,
                name: row.name,
                description: row.description,
                capacity: row.capacity,
                placement: row.placement.map(|value| CatalogWorkloadPlacement {
                    shape: value.shape.into_bytes(),
                    asset: value.asset,
                    enabled: value.enabled,
                    points: value
                        .points
                        .into_iter()
                        .map(|point| (point.x, point.y))
                        .collect(),
                }),
            })
            .collect())
    });
    sample
}
