//! Consumer-side serialization and admission controls for host-only metadata.

use std::collections::BTreeMap;

use ic_wire::{
    Error, native,
    typescript::{GenerationError, SchemaRegistry, generate_registry, native_schema},
};
use icydb::types::{Blob, Id, Ulid, Unit};
use serde_json::json;

use super::{ConsumerDto, ConsumerEntity, LIMITS, METHOD_ID, UlidEntity, sample};

#[test]
fn native_metadata_exposes_keys_without_entity_metadata_or_blob_policy() {
    assert_eq!(
        native_schema::<Id<ConsumerEntity>>(),
        native_schema::<u64>()
    );
    assert_eq!(native_schema::<Id<UlidEntity>>(), native_schema::<Ulid>());
    assert_eq!(native_schema::<Unit>(), Ok(json!({"k":"unit"})));
    assert_eq!(native_schema::<Blob>(), Ok(json!({"k":"bytes"})));
}

#[test]
fn a_blob_layout_cannot_supply_missing_endpoint_admission_limits() {
    let mut registry = SchemaRegistry::new();
    let schema = registry.register::<ConsumerDto>("ConsumerDto").unwrap();
    let types = registry.finish().unwrap();
    let method = json!({"name":"consumer_echo", "id":METHOD_ID.to_string(),
        "mode":"query", "request":schema, "response":schema});
    assert_eq!(
        generate_registry(
            &types,
            &[method],
            "./native-runtime.js",
            &BTreeMap::new(),
            |_| {}
        ),
        Err(GenerationError::Complexity),
    );
}

#[test]
fn consumer_frames_preserve_serde_bytes_in_both_directions() {
    let value = sample();
    // Postcard records are ordered fields. This independent primitive tuple
    // locks the existing serializers, without consulting metadata under test.
    let primitive = (
        value.id.key(),
        value.ulid.key(),
        (),
        ic_wire::ByteBuf::from(value.blob.as_bytes()),
        &value.address.0,
        &value.city.0,
        &value.postal.0,
        &value.region.0,
        &value.mime.0,
        &value.url.0,
        ic_wire::ByteBuf::from(value.utf8.0.as_bytes()),
        value.amount.0,
        value.degrees.0,
        value.percent.0,
        value.seconds.0,
    );
    let request = native::encode_request(METHOD_ID, &value, LIMITS).unwrap();
    assert_eq!(
        request,
        native::encode_request(METHOD_ID, &primitive, LIMITS).unwrap()
    );
    let response = native::encode_response(METHOD_ID, &value, LIMITS).unwrap();
    assert_eq!(
        response,
        native::encode_response(METHOD_ID, &primitive, LIMITS).unwrap()
    );
    assert_eq!(
        native::decode_request::<ConsumerDto>(METHOD_ID, &request, LIMITS).unwrap(),
        value
    );
    assert_eq!(
        native::decode_response::<ConsumerDto>(METHOD_ID, &response, LIMITS).unwrap(),
        value
    );
}

#[test]
fn native_consumer_refuses_malformed_and_oversized_frames() {
    let value = sample();
    let bytes = native::encode_request(METHOD_ID, &value, LIMITS).unwrap();
    for cut in 0..bytes.len() {
        assert!(native::decode_request::<ConsumerDto>(METHOD_ID, &bytes[..cut], LIMITS).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        native::decode_request::<ConsumerDto>(METHOD_ID, &trailing, LIMITS),
        Err(Error::Trailing)
    );
    assert_eq!(
        native::decode_response::<ConsumerDto>(METHOD_ID, &bytes, LIMITS),
        Err(Error::Identity)
    );
    let mut malformed = bytes;
    let text_start = malformed
        .windows(value.address.0.len())
        .position(|part| part == value.address.0.as_bytes())
        .unwrap();
    malformed[text_start] = 0xff;
    assert_eq!(
        native::decode_request::<ConsumerDto>(METHOD_ID, &malformed, LIMITS),
        Err(Error::Malformed)
    );
    let mut oversized = value;
    oversized.blob = Blob::from(vec![0; LIMITS.bytes]);
    assert_eq!(
        native::encode_request(METHOD_ID, &oversized, LIMITS),
        Err(Error::Limit)
    );
    // An allowed sender does not override the receiver's endpoint budget.
    let expanded = ic_wire::Limits {
        bytes: 2048,
        ..LIMITS
    };
    let bytes = native::encode_request(METHOD_ID, &oversized, expanded).unwrap();
    assert_eq!(
        native::decode_request::<ConsumerDto>(METHOD_ID, &bytes, LIMITS),
        Err(Error::Limit)
    );
}

#[test]
fn endpoint_budget_admits_empty_and_maximum_blob_without_global_blob_cap() {
    let mut value = sample();
    value.blob = Blob::default();
    let mut admitted = 0;
    // Locate the endpoint boundary using actual native framing (including the
    // variable length prefix), rather than inventing a universal Blob bound.
    for length in 0..=LIMITS.bytes {
        value.blob = Blob::from(vec![255; length]);
        match native::encode_request(METHOD_ID, &value, LIMITS) {
            Ok(bytes) => {
                assert_eq!(
                    native::decode_request::<ConsumerDto>(METHOD_ID, &bytes, LIMITS).unwrap(),
                    value
                );
                admitted = length;
            }
            Err(error) => {
                assert_eq!(error, Error::Limit);
                break;
            }
        }
    }
    assert!(admitted > 0);
}
