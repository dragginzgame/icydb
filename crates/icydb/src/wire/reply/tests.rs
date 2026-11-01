use crate::{
    Error, ErrorCode, ErrorOrigin,
    db::EntitySchemaDescription,
    diagnostic::RuntimeBoundaryCode,
    wire::{
        method::RpcMethod,
        reply::{
            MAX_PUBLIC_QUERY_REPLY_BYTES, MAX_REPLICATED_QUERY_REPLY_BYTES,
            into_deliverable_query_reply_with_limit, into_deliverable_schema_reply,
        },
    },
};

// A scalar label controls encoded size without changing container-prefix widths.
fn report(label_bytes: usize) -> Vec<EntitySchemaDescription> {
    vec![EntitySchemaDescription::new(
        "tests::Entry".into(),
        "x".repeat(label_bytes),
        1,
        1,
        [0; 16],
        "id".into(),
        vec!["id".into()],
        vec![],
        vec![],
        vec![],
        vec![],
        1,
        1,
    )]
}

#[test]
fn schema_reply_admission_preserves_small_and_empty_successes() {
    for candidate in [vec![], report(5)] {
        assert_eq!(
            into_deliverable_schema_reply(candidate.clone()),
            Ok(candidate)
        );
    }
}

#[test]
fn schema_reply_admission_measures_the_exact_frame_at_both_ic_caps() {
    for limit in [
        MAX_REPLICATED_QUERY_REPLY_BYTES,
        MAX_PUBLIC_QUERY_REPLY_BYTES,
    ] {
        let probe = report(limit - 256);
        let response: Result<&Vec<EntitySchemaDescription>, Error> = Ok(&probe);
        let overhead =
            ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
                .unwrap()
                - (limit - 256);
        for target in [limit - 1, limit, limit + 1] {
            let candidate = report(target - overhead);
            let response: Result<&Vec<EntitySchemaDescription>, Error> = Ok(&candidate);
            let bytes = ic_wire::native::encode_response(
                RpcMethod::Schema.id(),
                &response,
                icydb_schema::wire::sizing_limits(),
            )
            .unwrap();
            assert_eq!(bytes.len(), target);
            let result = into_deliverable_query_reply_with_limit(
                candidate.clone(),
                limit,
                RuntimeBoundaryCode::SchemaReplyBytesExceeded,
            );
            if target <= limit {
                assert_eq!(result, Ok(candidate));
            } else {
                let error = result.unwrap_err();
                assert_eq!(
                    error.code(),
                    ErrorCode::RUNTIME_BOUNDARY_SCHEMA_REPLY_BYTES_EXCEEDED
                );
                assert_eq!(error.origin(), ErrorOrigin::Response);
                let error_frame = RpcMethod::Schema
                    .encode_response(
                        Err::<Vec<EntitySchemaDescription>, Error>(error),
                        crate::wire::limits(limit),
                    )
                    .unwrap();
                assert!(error_frame.len() < limit);
                assert!(
                    RpcMethod::Schema
                        .decode_response::<Result<Vec<EntitySchemaDescription>, Error>>(
                            &error_frame,
                            crate::wire::limits(limit),
                        )
                        .unwrap()
                        .is_err()
                );
            }
        }
    }
}

#[test]
fn schema_public_guard_returns_a_deliverable_error_for_an_oversized_success() {
    let error = into_deliverable_schema_reply(report(MAX_PUBLIC_QUERY_REPLY_BYTES)).unwrap_err();
    assert_eq!(
        error.code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_REPLY_BYTES_EXCEEDED
    );
    let frame = RpcMethod::Schema
        .encode_response(
            Err::<Vec<EntitySchemaDescription>, Error>(error),
            crate::wire::limits(MAX_PUBLIC_QUERY_REPLY_BYTES),
        )
        .unwrap();
    assert!(frame.len() < MAX_REPLICATED_QUERY_REPLY_BYTES);
}
