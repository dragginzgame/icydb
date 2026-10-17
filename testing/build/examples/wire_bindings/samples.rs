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

pub(super) fn complete(tracer: &mut Tracer, samples: &Samples) -> Result<(), Box<dyn Error>> {
    // Serde reflection must visit nested enums independently. Adding a variant
    // or introducing a new nested enum fails generation until its metadata is complete.
    macro_rules! trace {
        ($ty:ty) => {
            tracer.trace_type::<$ty>(samples)?;
        };
    }
    trace!(icydb::Error);
    trace!(icydb::value::PublicValue);
    trace!(icydb::db::sql::SqlQueryResult);
    trace!(icydb::db::IntegrityCheckResult);
    trace!(icydb::db::SqlIntegrityError);
    trace!(icydb::metrics::MetricsReport);
    trace!(icydb::db::StorageReport);
    trace!(Vec<icydb::db::EntitySchemaDescription>);
    trace!(icydb::db::SchemaMigrationCommand);
    trace!(icydb::db::SchemaMigrationStatusRequest);
    trace!(icydb::db::SchemaMigrationStatusPage);
    trace!(icydb::db::IntegrityCheckError);
    trace!(icydb::ConstraintValuePathComponent);
    trace!(icydb_core::db::DeepIntegrityPageStatus);
    trace!(icydb_core::db::EntityRelationCardinality);
    trace!(icydb::db::ExpectedAcceptedHead);
    trace!(icydb_core::db::IndexState);
    trace!(icydb_core::db::IntegrityFindingClass);
    trace!(icydb_core::db::IntegrityFindingKind);
    trace!(icydb_core::db::IntegrityJobReceipt);
    trace!(icydb_core::db::IntegrityPhase);
    trace!(icydb_core::db::IntegritySeverity);
    trace!(icydb_core::db::IntegrityVerifierFamily);
    trace!(icydb_core::db::MemoryAllocationBinding);
    trace!(icydb_core::db::QuickIntegrityStatus);
    trace!(icydb_core::db::SchemaMigrationFindingKind);
    trace!(icydb_core::db::SchemaMigrationPhase);
    trace!(icydb_core::db::SqlColumnDefault);
    trace!(icydb_core::db::SqlColumnExtra);
    trace!(icydb_core::db::SqlColumnKey);
    trace!(icydb_core::db::SqlDescribeOutput);
    trace!(icydb_core::db::SqlShowColumnsOutput);
    trace!(icydb_core::db::StoreAllocationIdentityCapability);
    trace!(icydb_core::db::StoreCommitParticipation);
    trace!(icydb_core::db::StoreDurability);
    trace!(icydb_core::db::StoreRecoveryCapability);
    trace!(icydb_core::db::StoreSchemaMetadataCapability);
    trace!(icydb_core::db::StoreSnapshotStorageMode);
    trace!(icydb_core::db::IntegrityAbortStatus);
    trace!(icydb_core::db::IntegrityAuthorityClass);
    trace!(icydb_core::db::IntegrityJobError);
    trace!(icydb_core::db::IntegrityPendingTerminal);
    trace!(icydb_core::db::IntegrityTerminalOutcome);
    Ok(())
}
