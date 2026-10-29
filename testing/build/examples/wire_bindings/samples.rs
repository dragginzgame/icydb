//! Concrete native contracts and valid scalar samples; never infer arbitrary Serde bounds.

use std::error::Error;

use icydb::{
    types::*,
    value::{PublicEnumValue, PublicValue as V},
};
use serde_reflection::{Samples, Tracer};

pub(super) fn values() -> Result<Vec<V>, Box<dyn Error>> {
    Ok(vec![
        V::Account(Account::from_owner_and_subaccount(
            Principal::anonymous(),
            Some(Subaccount::MIN),
        )),
        V::Blob(vec![0]),
        V::Bool(true),
        V::Date(Date::default()),
        V::Decimal(Decimal::ZERO),
        V::Duration(Duration::default()),
        V::Enum(PublicEnumValue::new("Value", Some("Example")).with_payload(V::Null)),
        V::Float32(Float32::try_new(0.0).ok_or("invalid float sample")?),
        V::Float64(Float64::try_new(0.0).ok_or("invalid float sample")?),
        V::Int64(0),
        V::Int128(0),
        V::List(vec![V::Null]),
        V::Map(vec![(V::Null, V::Null)]),
        V::Null,
        V::Principal(Principal::anonymous()),
        V::Subaccount(Subaccount::MIN),
        V::Text("0".into()),
        V::Timestamp(Timestamp::default()),
        V::Nat64(0),
        V::Nat128(0),
        V::Ulid(Ulid::MIN),
        V::Unit,
        V::U256(U256::ZERO),
    ])
}

pub(super) fn seed(tracer: &mut Tracer, samples: &mut Samples) -> Result<(), Box<dyn Error>> {
    // Numeric transport validates restoration; reflection must use admitted states.
    let mut measured_zero = ic_metrics::MeasurementAggregate::EMPTY;
    measured_zero.record(0);
    tracer.trace_value(samples, &measured_zero)?;
    tracer.trace_value(samples, &ic_metrics::MeasurementAggregate::EMPTY)?;
    let values = values()?;
    for value in &values {
        tracer.trace_value(samples, value)?;
    }
    // Retain the complete Some-byte metadata, then use a None sample because
    // reflection's sample deserializer does not adapt byte strings to sequences.
    tracer.trace_value(
        samples,
        &Account::from_owner_and_subaccount(Principal::anonymous(), None),
    )?;
    Ok(())
}
