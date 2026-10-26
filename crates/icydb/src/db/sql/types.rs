//! Module: db::sql::types
//!
//! Responsibility: public native SQL results.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::{
    ConstraintValidationFindingOutput, Error, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{
        EntityCatalogDescription, EntityConstraintDescription, MemoryCatalogDescription,
        RowProjectionOutput, SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput,
        StoreCatalogDescription,
    },
};

use serde::Deserialize;

const MAX_PUBLIC_SQL_QUERY_REPLY_BYTES: usize = icydb_schema::wire::MAX_REPLY_BYTES;
#[cfg(any(target_arch = "wasm32", test))]
const MAX_REPLICATED_SQL_QUERY_REPLY_BYTES: usize = 2 * 1024 * 1024;

#[cfg_attr(doc, doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload.")]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub struct SqlGroupedRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    /// SQL display cells: NULL is uppercase and marker-like text is quoted.
    /// Values are formatted before conversion; consumers must not infer types
    /// from these strings or rewrite them.
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
    pub next_cursor: Option<String>,
}

#[cfg_attr(
    doc,
    doc = "SqlConstraintValidationOutput\n\nTyped progress from one bounded constraint-validation step."
)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub struct SqlConstraintValidationOutput {
    /// Stable accepted constraint identity.
    pub constraint_id: u32,
    /// Durable activation identity while validation remains active.
    pub activation_epoch: Option<u64>,
    /// Sequence to acknowledge before advancing a retained finding page.
    pub page_sequence: Option<u64>,
    /// Current engine-owned validation state.
    pub state: String,
    /// Current revision-proof status.
    pub revision_status: String,
    /// Cumulative classified-row count for this job.
    pub rows_scanned: u64,
    /// Bounded findings retained by this page.
    pub findings: Vec<ConstraintValidationFindingOutput>,
    /// Whether validation and accepted publication are complete.
    pub complete: bool,
}

#[cfg_attr(doc, doc = "SqlQueryResult\n\nUnified SQL endpoint result.")]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
pub enum SqlQueryResult {
    Count {
        entity: String,
        row_count: u32,
    },
    Projection(RowProjectionOutput),
    Grouped(SqlGroupedRowsOutput),
    Explain {
        entity: String,
        explain: String,
    },
    Describe(SqlDescribeOutput),
    ShowConstraints {
        entity: String,
        constraints: Vec<EntityConstraintDescription>,
    },
    ShowIndexes {
        entity: String,
        indexes: Vec<String>,
    },
    ShowColumns(SqlShowColumnsOutput),
    ShowRelations(SqlShowRelationsOutput),
    ShowEntities {
        entities: Vec<EntityCatalogDescription>,
        verbose: bool,
    },
    ShowStores {
        stores: Vec<StoreCatalogDescription>,
        verbose: bool,
    },
    ShowMemory {
        memory: Vec<MemoryCatalogDescription>,
    },
    Ddl {
        entity: String,
        mutation_kind: String,
        target_index: String,
        target_store: String,
        field_path: Vec<String>,
        status: String,
        rows_scanned: u64,
        index_keys_written: u64,
        /// Typed progress when this statement advances constraint validation.
        constraint_validation: Option<SqlConstraintValidationOutput>,
    },
}

impl SqlQueryResult {
    // Measure the exact native success payload plus its IC Wire prefix.
    // Core calls this with an empty canonical projection to establish framing.
    pub(in crate::db) fn encoded_returning_response_len(
        projection: RowProjectionOutput,
    ) -> Result<usize, icydb_schema::wire::Error> {
        let response: Result<Self, Error> = Ok(Self::Projection(projection));
        ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
    }

    /// Reject a success whose exact public Wire envelope cannot be delivered
    /// by the deployed IC query boundary.
    #[doc(hidden)]
    pub fn into_deliverable_query_reply(self) -> Result<Self, Error> {
        // The same query export also runs in replicated execution. Use the
        // actual message context rather than the method's query declaration.
        #[cfg(target_arch = "wasm32")]
        let max_reply_bytes = if ic_platform::ic0::in_replicated_execution() != 0 {
            MAX_REPLICATED_SQL_QUERY_REPLY_BYTES
        } else {
            MAX_PUBLIC_SQL_QUERY_REPLY_BYTES
        };
        #[cfg(not(target_arch = "wasm32"))]
        let max_reply_bytes = MAX_PUBLIC_SQL_QUERY_REPLY_BYTES;

        self.into_deliverable_query_reply_with_limit(max_reply_bytes)
    }

    // Keep complete-envelope admission shared by both execution contexts and
    // directly testable without a host-side IC system-call stub.
    fn into_deliverable_query_reply_with_limit(
        self,
        max_reply_bytes: usize,
    ) -> Result<Self, Error> {
        let response: Result<&Self, Error> = Ok(&self);
        let encoded =
            ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
                .map_err(|_| {
                    Error::from_kind(
                        ErrorKind::Runtime(RuntimeErrorKind::Internal),
                        ErrorOrigin::Response,
                    )
                })?;
        if encoded > max_reply_bytes {
            return Err(Error::from_runtime_boundary(
                crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
                ErrorOrigin::Response,
            ));
        }

        Ok(self)
    }
}

#[cfg(test)]
mod tests {

    use super::{
        MAX_PUBLIC_SQL_QUERY_REPLY_BYTES, MAX_REPLICATED_SQL_QUERY_REPLY_BYTES, SqlQueryResult,
    };
    use crate::{Error, ErrorOrigin};

    #[test]
    fn returning_frame_counts_match_the_actual_public_response() {
        use crate::{
            db::RowProjectionOutput,
            value::{OutputValue, PublicValue},
        };

        for (count, size) in [
            (0, 0),
            (1, 127),
            (1, 128),
            (127, 128),
            (128, 128),
            (1, 1_048_000),
            (100, 10_300),
        ] {
            let row = vec![
                OutputValue::text("x".repeat(size)),
                OutputValue::list(vec![PublicValue::Null, PublicValue::Bool(true)]),
            ];
            let output = RowProjectionOutput {
                entity: "Row".into(),
                columns: vec!["text".into(), "nested".into()],
                rows: vec![row; count],
                row_count: u32::try_from(count).unwrap(),
            };
            let mut empty = output.clone();
            empty.rows.clear();
            let empty_frame = SqlQueryResult::encoded_returning_response_len(empty).unwrap();
            let values_len = icydb_schema::wire::encoded_len(&output.rows).unwrap();
            let candidate: Result<SqlQueryResult, Error> = Ok(SqlQueryResult::Projection(output));
            let actual = icydb_schema::wire::encode_one(candidate).unwrap().len() + 11;
            // The empty vector occupies one byte; the real vector contains its
            // own length prefix and each row's value encoding exactly once.
            assert_eq!(empty_frame - 1 + values_len, actual);
        }
    }

    #[test]
    fn generated_sql_query_reply_guard_measures_the_exact_complete_envelope() {
        let small = SqlQueryResult::ShowIndexes {
            entity: "Entry".to_string(),
            indexes: vec!["by_owner".to_string()],
        };
        let owned: Result<SqlQueryResult, Error> = Ok(small.clone());
        let borrowed: Result<&SqlQueryResult, Error> = Ok(&small);

        assert_eq!(
            icydb_schema::wire::encode_one(owned).expect("owned endpoint envelope should encode"),
            icydb_schema::wire::encode_one(borrowed)
                .expect("borrowed endpoint envelope should encode"),
            "the zero-copy guard must measure the exact generated endpoint envelope",
        );
        assert_eq!(
            small.into_deliverable_query_reply(),
            Ok(SqlQueryResult::ShowIndexes {
                entity: "Entry".to_string(),
                indexes: vec!["by_owner".to_string()],
            })
        );
    }

    #[test]
    fn generated_sql_query_reply_guard_matches_execution_boundaries() {
        for limit in [
            MAX_REPLICATED_SQL_QUERY_REPLY_BYTES,
            MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        ] {
            let probe_len = limit - 128;
            let probe = SqlQueryResult::ShowIndexes {
                entity: "Entry".into(),
                indexes: vec!["x".repeat(probe_len)],
            };
            let response: Result<&SqlQueryResult, Error> = Ok(&probe);
            let overhead =
                ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
                    .unwrap()
                    - probe_len;
            for target in [limit - 1, limit, limit + 1] {
                let candidate = SqlQueryResult::ShowIndexes {
                    entity: "Entry".into(),
                    indexes: vec!["x".repeat(target - overhead)],
                };
                let response: Result<&SqlQueryResult, Error> = Ok(&candidate);
                let encoded = ic_wire::native::encode_response(
                    canic_icydb::wire::Endpoint::Query.id(),
                    &response,
                    icydb_schema::wire::sizing_limits(),
                )
                .unwrap();
                assert_eq!(encoded.len(), target);
                let result = candidate
                    .clone()
                    .into_deliverable_query_reply_with_limit(limit);
                if target <= limit {
                    assert_eq!(result, Ok(candidate));
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error.code(),
                        crate::ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED
                    );
                    assert_eq!(error.origin(), ErrorOrigin::Response);
                    assert!(
                        canic_icydb::wire::Endpoint::Query
                            .encode_response(
                                Err::<SqlQueryResult, Error>(error),
                                crate::wire::limits(crate::wire::MAX_REPLY_BYTES)
                            )
                            .unwrap()
                            .len()
                            < limit
                    );
                }
            }
        }
    }

    #[test]
    fn generated_sql_query_reply_guard_preserves_codec_refusal() {
        for limit in [
            MAX_REPLICATED_SQL_QUERY_REPLY_BYTES,
            MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        ] {
            let candidate = SqlQueryResult::ShowIndexes {
                entity: "Entry".into(),
                indexes: vec![String::new(); icydb_schema::wire::MAX_NODES],
            };
            let error = candidate
                .into_deliverable_query_reply_with_limit(limit)
                .unwrap_err();
            assert_eq!(error.code(), crate::ErrorCode::RUNTIME_INTERNAL);
            assert_eq!(error.origin(), ErrorOrigin::Response);
        }
    }

    #[test]
    fn generated_sql_query_reply_guard_returns_a_deliverable_typed_error() {
        let oversized = SqlQueryResult::ShowIndexes {
            entity: "Entry".to_string(),
            indexes: vec!["x".repeat(MAX_PUBLIC_SQL_QUERY_REPLY_BYTES)],
        };
        let candidate: Result<&SqlQueryResult, Error> = Ok(&oversized);
        assert!(
            icydb_schema::wire::encoded_len(&candidate)
                .expect("oversized endpoint candidate should remain fallibly sizeable")
                > MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        );

        let error = oversized
            .into_deliverable_query_reply()
            .expect_err("oversized success must become a typed response error");
        assert_eq!(
            error.code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED,
        );
        assert_eq!(error.origin(), ErrorOrigin::Response);
        assert!(
            icydb_schema::wire::encode_one(Err::<SqlQueryResult, Error>(error))
                .expect("typed oversize error should encode")
                .len()
                <= MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        );
    }
}
