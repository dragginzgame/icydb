//! Current endpoint envelopes and native projections.

use crate::__macro::RpcMethod;

#[test]
fn endpoint_decoders_accept_deserialize_only_arguments() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct ReceiveOnly(u32);

    let request = RpcMethod::Query
        .encode_request(128_u32, crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(
        RpcMethod::Query.decode_request::<ReceiveOnly>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
    let response = RpcMethod::Query
        .encode_response(128_u32, crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(
        RpcMethod::Query.decode_response::<ReceiveOnly>(
            &response,
            crate::wire::limits(crate::wire::MAX_REPLY_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
}

#[test]
fn endpoint_identity_and_direction_are_checked_before_payloads() {
    let request = RpcMethod::Metrics
        .encode_request((), crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(&request[..2], b"IW");
    assert_eq!(&request[2..10], &RpcMethod::Metrics.id().to_le_bytes());
    assert_eq!(request[10], 0);
    assert_eq!(
        RpcMethod::Metrics.decode_request::<()>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(())
    );
    assert_eq!(
        RpcMethod::Snapshot.decode_request::<()>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Err(ic_wire::Error::Identity)
    );
    assert_eq!(
        RpcMethod::Metrics
            .decode_response::<()>(&request, crate::wire::limits(crate::wire::MAX_REPLY_BYTES)),
        Err(ic_wire::Error::Identity)
    );

    let response = RpcMethod::Metrics
        .encode_response((), crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(response[10], 1);
    assert_eq!(
        RpcMethod::Metrics
            .decode_response::<()>(&response, crate::wire::limits(crate::wire::MAX_REPLY_BYTES)),
        Ok(())
    );
}

#[test]
fn borrowed_arguments_share_the_generated_native_request_projection() {
    let sql = "SELECT id FROM Item".to_owned();
    let request = RpcMethod::Query
        .encode_request((&sql,), crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(
        RpcMethod::Query
            .decode_request::<String>(
                &request,
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .unwrap(),
        sql
    );
    assert!(
        RpcMethod::Query
            .decode_request::<String>(
                &request[..request.len() - 1],
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .is_err()
    );
    let mut trailing = request;
    trailing.push(0);
    assert!(
        RpcMethod::Query
            .decode_request::<String>(
                &trailing,
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .is_err()
    );
}

#[test]
fn typed_public_errors_round_trip_through_the_actual_endpoint_envelope() {
    let error = crate::Error::from_runtime_boundary(
        crate::diagnostic::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
        crate::ErrorOrigin::Runtime,
    );
    let response: Result<(), crate::Error> = Err(error);
    let bytes = RpcMethod::MetricsReset
        .encode_response(&response, crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(
        RpcMethod::MetricsReset
            .decode_response::<Result<(), crate::Error>>(
                &bytes,
                crate::wire::limits(crate::wire::MAX_REPLY_BYTES)
            )
            .unwrap(),
        response
    );
}
