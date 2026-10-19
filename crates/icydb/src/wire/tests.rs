//! Current endpoint envelopes and native projections.

use crate::wire::{Endpoint, ID_BASE};

#[test]
fn endpoint_identity_and_direction_are_checked_before_payloads() {
    let request = Endpoint::Metrics.encode_request(()).unwrap();
    assert_eq!(&request[..2], b"IW");
    assert_eq!(&request[2..10], &Endpoint::Metrics.id().to_le_bytes());
    assert_eq!(request[10], 0);
    assert_eq!(Endpoint::Metrics.decode_request::<()>(&request), Ok(()));
    assert_eq!(
        Endpoint::Snapshot.decode_request::<()>(&request),
        Err(ic_wire::Error::Identity)
    );
    assert_eq!(
        Endpoint::Metrics.decode_response::<()>(&request),
        Err(ic_wire::Error::Identity)
    );

    let response = Endpoint::Metrics.encode_response(()).unwrap();
    assert_eq!(response[10], 1);
    assert_eq!(Endpoint::Metrics.decode_response::<()>(&response), Ok(()));
}

#[test]
fn borrowed_arguments_share_the_generated_native_request_projection() {
    let sql = "SELECT id FROM Item".to_owned();
    let request = Endpoint::Query.encode_request((&sql,)).unwrap();
    assert_eq!(
        Endpoint::Query.decode_request::<String>(&request).unwrap(),
        sql
    );
    assert!(
        Endpoint::Query
            .decode_request::<String>(&request[..request.len() - 1])
            .is_err()
    );
    let mut trailing = request;
    trailing.push(0);
    assert!(Endpoint::Query.decode_request::<String>(&trailing).is_err());
}

#[test]
fn lifecycle_arguments_bind_their_phase_and_native_types() {
    let arguments = ("fixture".to_owned(), 42_u64);
    let bytes = crate::wire::encode_init_arguments(&arguments).unwrap();
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
    let upgrade = crate::wire::encode_post_upgrade_arguments(&arguments).unwrap();
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
    let bytes = Endpoint::MetricsReset.encode_response(&response).unwrap();
    assert_eq!(
        Endpoint::MetricsReset
            .decode_response::<Result<(), crate::Error>>(&bytes)
            .unwrap(),
        response
    );
}
