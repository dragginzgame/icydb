use super::{SqlQueryPageResult, SqlQueryResult};
use crate::wire::reply::{
    MAX_PUBLIC_QUERY_REPLY_BYTES as MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
    MAX_REPLICATED_QUERY_REPLY_BYTES as MAX_REPLICATED_SQL_QUERY_REPLY_BYTES,
    into_deliverable_query_reply_with_limit,
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
        icydb_schema::wire::encode_one(borrowed).expect("borrowed endpoint envelope should encode"),
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
                crate::__macro::RpcMethod::Query.id(),
                &response,
                icydb_schema::wire::sizing_limits(),
            )
            .unwrap();
            assert_eq!(encoded.len(), target);
            let result = into_deliverable_query_reply_with_limit(
                candidate.clone(),
                limit,
                crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
            );
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
                    crate::__macro::RpcMethod::Query
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
        let error = into_deliverable_query_reply_with_limit(
            candidate,
            limit,
            crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
        )
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

#[test]
fn public_sql_reply_guard_includes_cursor_and_work_in_the_complete_frame() {
    let page = |cursor_bytes| SqlQueryPageResult::Scalar {
        projection: crate::db::RowProjectionOutput {
            entity: "Entry".into(),
            columns: vec!["id".into()],
            rows: Vec::new(),
            row_count: 0,
        },
        continuation: Some("x".repeat(cursor_bytes)),
        work: crate::db::ScalarPageWork {
            envelope_identity: 1,
            entries_visited: 10_000,
            result_rows: 0,
        },
    };
    assert_eq!(page(128).into_deliverable_query_reply(), Ok(page(128)));
    let maximum = MAX_PUBLIC_SQL_QUERY_REPLY_BYTES;
    let probe = page(maximum - 128);
    let response: Result<&SqlQueryPageResult, Error> = Ok(&probe);
    let overhead =
        ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits()).unwrap()
            - (maximum - 128);
    for length in [maximum - 1, maximum, maximum + 1] {
        let candidate = page(length - overhead);
        let response: Result<&SqlQueryPageResult, Error> = Ok(&candidate);
        let bytes = ic_wire::native::encode_response(
            crate::__macro::RpcMethod::Query.id(),
            &response,
            icydb_schema::wire::sizing_limits(),
        )
        .unwrap();
        assert_eq!(bytes.len(), length);
        let result = candidate.clone().into_deliverable_query_reply();
        if length <= maximum {
            assert_eq!(result, Ok(candidate));
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.code(),
                crate::ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED,
            );
            assert_eq!(error.origin(), ErrorOrigin::Response);
        }
    }
}
