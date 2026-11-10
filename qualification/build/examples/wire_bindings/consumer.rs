//! Qualifies consumer DTO composition independently of IcyDB's endpoint roster.
//! Owns a fixture endpoint and its policy, never production method identities.

use std::{collections::BTreeMap, error::Error, fs, path::Path};

use ic_wire::{Limits, native, typescript};
use icydb::{
    model::base::types::{bytes, finance, geo, num, time, web},
    types::{Blob, Decimal, Id, Ulid, Unit},
};
use icydb_core::db::EntityKey;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_reflection::{Samples, Tracer, TracerConfig};

use crate::vectors;

// Deliberately has no Serialize or Wire implementation: Id exposes only its key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ConsumerEntity;

impl EntityKey for ConsumerEntity {
    type Key = u64;
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct UlidEntity;

impl EntityKey for UlidEntity {
    type Key = Ulid;
}

#[derive(Debug, Deserialize, PartialEq, Serialize, ic_wire::Wire)]
struct ConsumerDto {
    id: Id<ConsumerEntity>,
    ulid: Id<UlidEntity>,
    unit: Unit,
    blob: Blob,
    address: geo::AddressLine,
    city: geo::CityName,
    postal: geo::PostalCode,
    region: geo::RegionName,
    mime: web::MimeType,
    url: web::Url,
    utf8: bytes::Utf8,
    amount: finance::Usd,
    degrees: num::Degrees,
    percent: num::Percent,
    seconds: time::Seconds,
}

const METHOD_ID: u64 = 628;
const LIMITS: Limits = Limits {
    bytes: 512,
    // Native admission charges the owned record's in-memory shells as well as
    // text/byte backing; this is independent of the 512-byte frame ceiling.
    allocation: 64 * 1024,
    nodes: 256,
    depth: 16,
};

fn sample() -> ConsumerDto {
    ConsumerDto {
        id: Id::from_key(u64::MAX),
        ulid: Id::from_key(Ulid::from_u128(628)),
        unit: Unit,
        blob: Blob::from(vec![0, 128, 255]),
        address: geo::AddressLine::from("1 rue é🦀".to_owned()),
        city: geo::CityName::from("Monaco".to_owned()),
        postal: geo::PostalCode::from("98000".to_owned()),
        region: geo::RegionName::from("MC".to_owned()),
        mime: web::MimeType::from("application/octet-stream".to_owned()),
        url: web::Url::from("https://example.com/é".to_owned()),
        utf8: bytes::Utf8::from(Blob::from("é🦀".as_bytes().to_vec())),
        amount: finance::Usd::from(Decimal::new(12345, 2)),
        degrees: num::Degrees::from(359_u16),
        percent: num::Percent::from(99_u8),
        seconds: time::Seconds::from(u64::MAX),
    }
}

pub(super) fn write(out: &Path) -> Result<(), Box<dyn Error>> {
    let mut registry = typescript::SchemaRegistry::new();
    let schema = registry
        .register::<ConsumerDto>("ConsumerDto")
        .map_err(|error| format!("consumer metadata: {error:?}"))?;
    let types = registry
        .finish()
        .map_err(|error| format!("consumer metadata: {error:?}"))?;
    let mut method = typescript::method_limits(LIMITS, LIMITS)
        .map_err(|error| format!("consumer limits: {error:?}"))?;
    method["name"] = json!("consumer_echo");
    method["id"] = json!(METHOD_ID.to_string());
    method["mode"] = json!("query");
    method["request"] = schema.clone();
    method["response"] = schema;
    let source = typescript::generate_registry(
        &types,
        &[method.clone()],
        "./native-runtime.js",
        &BTreeMap::new(),
        |report| assert!(report.is_clean(), "fixture must have no bulk warnings"),
    )
    .map_err(|error| format!("consumer generation: {error:?}"))?;
    fs::write(out.join("consumer.ts"), source)?;
    let value = sample();
    let (_, reflected) =
        Tracer::new(TracerConfig::default()).trace_value(&mut Samples::new(), &value)?;
    fs::write(
        out.join("consumer-vectors.mjs"),
        format!(
            "export const value = {};\nexport const request = new Uint8Array({});\nexport const response = new Uint8Array({});\n",
            vectors::js(&method["request"], &reflected, &types)?,
            serde_json::to_string(&native::encode_request(METHOD_ID, &value, LIMITS)?)?,
            serde_json::to_string(&native::encode_response(METHOD_ID, &value, LIMITS)?)?,
        ),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
