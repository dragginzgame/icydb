//! Native client contracts interoperate with the published host adapter.

use canic_icydb::wire::Endpoint;
use icydb::__macro::RpcMethod;

#[test]
fn native_rpc_methods_preserve_the_published_adapter_contract() {
    let limits = icydb::wire::limits(4096);
    let arguments = ("database".to_owned(), 128_u64);

    for method in RpcMethod::ALL {
        let endpoint = Endpoint::from_name(method.name()).expect("published method must exist");
        assert_eq!(method.id(), endpoint.id());
        assert_eq!(RpcMethod::from_name(method.name()), Some(*method));
        assert_eq!(RpcMethod::from_id(method.id()), Some(*method));

        let request = method.encode_request(&arguments, limits).unwrap();
        assert_eq!(
            request,
            endpoint.encode_request(&arguments, limits).unwrap()
        );
        assert_eq!(
            endpoint
                .decode_request::<(String, u64)>(&request, limits)
                .unwrap(),
            arguments,
        );

        let reply = endpoint.encode_response(&arguments, limits).unwrap();
        assert_eq!(reply, method.encode_response(&arguments, limits).unwrap());
        assert_eq!(
            method
                .decode_response::<(String, u64)>(&reply, limits)
                .unwrap(),
            arguments,
        );
    }
}

const ID_BASE: u64 = u64::from_le_bytes(*b"ICYDBW00");

#[test]
fn lifecycle_arguments_bind_their_phase_and_native_types() {
    let arguments = ("fixture".to_owned(), 42_u64);
    let bytes = canic_icydb::wire::encode_init_arguments(
        &arguments,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    let decoded = ic_wire::native::decode_request::<(String, u64)>(
        ID_BASE + 13,
        &bytes,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(decoded, arguments);
    assert!(
        ic_wire::native::decode_request::<(String, u64)>(
            ID_BASE + 14,
            &bytes,
            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
        )
        .is_err()
    );
    let upgrade = canic_icydb::wire::encode_post_upgrade_arguments(
        &arguments,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    let decoded = ic_wire::native::decode_request::<(String, u64)>(
        ID_BASE + 14,
        &upgrade,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(decoded, arguments);
}

#[test]
fn lifecycle_decoders_accept_deserialize_only_arguments() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct ReceiveOnly(u32);
    let init = canic_icydb::wire::encode_init_arguments(
        128_u32,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(
        canic_icydb::wire::decode_init_arguments::<ReceiveOnly>(
            &init,
            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
    let upgrade = canic_icydb::wire::encode_post_upgrade_arguments(
        128_u32,
        icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
    )
    .unwrap();
    assert_eq!(
        canic_icydb::wire::decode_post_upgrade_arguments::<ReceiveOnly>(
            &upgrade,
            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
        ),
        Ok(ReceiveOnly(128))
    );
}
