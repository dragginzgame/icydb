//! Current endpoint envelopes and native projections.

use canic_icydb::wire::Endpoint;

const ID_BASE: u64 = u64::from_le_bytes(*b"ICYDBW00");

#[test]
fn endpoint_and_lifecycle_decoders_accept_deserialize_only_arguments() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct ReceiveOnly(u32);

    let request = Endpoint::Query
        .encode_request(128_u32, crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(
        Endpoint::Query.decode_request::<ReceiveOnly>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
    let response = Endpoint::Query
        .encode_response(128_u32, crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(
        Endpoint::Query.decode_response::<ReceiveOnly>(
            &response,
            crate::wire::limits(crate::wire::MAX_REPLY_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
    let init = canic_icydb::wire::encode_init_arguments(
        128_u32,
        crate::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(
        canic_icydb::wire::decode_init_arguments::<ReceiveOnly>(
            &init,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
    let upgrade = canic_icydb::wire::encode_post_upgrade_arguments(
        128_u32,
        crate::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(
        canic_icydb::wire::decode_post_upgrade_arguments::<ReceiveOnly>(
            &upgrade,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
}

#[test]
fn endpoint_identity_and_direction_are_checked_before_payloads() {
    let request = Endpoint::Metrics
        .encode_request((), crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(&request[..2], b"IW");
    assert_eq!(&request[2..10], &Endpoint::Metrics.id().to_le_bytes());
    assert_eq!(request[10], 0);
    assert_eq!(
        Endpoint::Metrics.decode_request::<()>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Ok(())
    );
    assert_eq!(
        Endpoint::Snapshot.decode_request::<()>(
            &request,
            crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        ),
        Err(ic_wire::Error::Identity)
    );
    assert_eq!(
        Endpoint::Metrics
            .decode_response::<()>(&request, crate::wire::limits(crate::wire::MAX_REPLY_BYTES)),
        Err(ic_wire::Error::Identity)
    );

    let response = Endpoint::Metrics
        .encode_response((), crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(response[10], 1);
    assert_eq!(
        Endpoint::Metrics
            .decode_response::<()>(&response, crate::wire::limits(crate::wire::MAX_REPLY_BYTES)),
        Ok(())
    );
}

#[test]
fn borrowed_arguments_share_the_generated_native_request_projection() {
    let sql = "SELECT id FROM Item".to_owned();
    let request = Endpoint::Query
        .encode_request((&sql,), crate::wire::limits(crate::wire::MAX_REQUEST_BYTES))
        .unwrap();
    assert_eq!(
        Endpoint::Query
            .decode_request::<String>(
                &request,
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .unwrap(),
        sql
    );
    assert!(
        Endpoint::Query
            .decode_request::<String>(
                &request[..request.len() - 1],
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .is_err()
    );
    let mut trailing = request;
    trailing.push(0);
    assert!(
        Endpoint::Query
            .decode_request::<String>(
                &trailing,
                crate::wire::limits(crate::wire::MAX_REQUEST_BYTES)
            )
            .is_err()
    );
}

#[test]
fn lifecycle_arguments_bind_their_phase_and_native_types() {
    let arguments = ("fixture".to_owned(), 42_u64);
    let bytes = canic_icydb::wire::encode_init_arguments(
        &arguments,
        crate::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    let decoded = ic_wire::native::decode_request::<(String, u64)>(
        ID_BASE + 13,
        &bytes,
        icydb_schema::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(decoded, arguments);
    assert!(
        ic_wire::native::decode_request::<(String, u64)>(
            ID_BASE + 14,
            &bytes,
            icydb_schema::wire::limits(crate::wire::MAX_REQUEST_BYTES)
        )
        .is_err()
    );
    let upgrade = canic_icydb::wire::encode_post_upgrade_arguments(
        &arguments,
        crate::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    let decoded = ic_wire::native::decode_request::<(String, u64)>(
        ID_BASE + 14,
        &upgrade,
        icydb_schema::wire::limits(crate::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(decoded, arguments);
}

#[test]
fn typed_public_errors_round_trip_through_the_actual_endpoint_envelope() {
    let error = crate::Error::from_runtime_boundary(
        crate::diagnostic::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
        crate::ErrorOrigin::Runtime,
    );
    let response: Result<(), crate::Error> = Err(error);
    let bytes = Endpoint::MetricsReset
        .encode_response(&response, crate::wire::limits(crate::wire::MAX_REPLY_BYTES))
        .unwrap();
    assert_eq!(
        Endpoint::MetricsReset
            .decode_response::<Result<(), crate::Error>>(
                &bytes,
                crate::wire::limits(crate::wire::MAX_REPLY_BYTES)
            )
            .unwrap(),
        response
    );
}
