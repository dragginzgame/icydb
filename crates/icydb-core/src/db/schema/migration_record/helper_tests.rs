//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db) fn prepared_schema_migration_record_op_for_tests()
-> Result<SchemaMigrationRecordOp, InternalError> {
    Ok(schema_migration_record_lifecycle_ops_for_tests()?.0)
}

pub(in crate::db) fn schema_migration_record_lifecycle_ops_for_tests() -> Result<
    (
        SchemaMigrationRecordOp,
        SchemaMigrationRecordOp,
        SchemaMigrationRecordOp,
    ),
    InternalError,
> {
    let record = SchemaMigrationRecord::prepared(
        TargetDatabaseIdentity::from_bytes([0x91; 32]),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x92; 32]),
        },
        ExpectedAcceptedHead::Exact {
            revision: 2,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x93; 32]),
        },
        SchemaProposalDigest::from_bytes([0x94; 32]),
        SchemaMigrationPlanDigest::from_bytes([0x95; 32]),
        vec![PersistedSchemaMigrationTransition::try_new(
            EntitySourceKey::try_new("RecoveryEntity")
                .map_err(|_| InternalError::store_invariant())?,
            1,
            2,
        )?],
        vec![PersistedSchemaMigrationEntity::try_new(
            TargetStoreIdentity::from_bytes([0x96; 32]),
            EntityTag::new(1),
            EntitySourceDigest::from_bytes([0x97; 32]),
        )?],
        Vec::new(),
    )?;
    let validating = record.transition(
        PersistedSchemaMigrationPhase::Validating,
        PersistedSchemaMigrationProgress::default(),
    )?;
    let aborted = validating.transition(
        PersistedSchemaMigrationPhase::Aborted,
        PersistedSchemaMigrationProgress::default(),
    )?;
    Ok((
        SchemaMigrationRecordOp::insert(&record)?,
        SchemaMigrationRecordOp::replace(&record, &validating)?,
        SchemaMigrationRecordOp::replace(&validating, &aborted)?,
    ))
}

pub(in crate::db) fn physical_migration_record_ops_for_tests(
    first_entity: EntityTag,
    entity_count: u64,
) -> Result<Vec<SchemaMigrationRecordOp>, InternalError> {
    let initial = prepared_schema_migration_record_op_for_tests()?;
    let mut record = decode_schema_migration_record(initial.after_bytes())?;
    record.entities = (1..=entity_count)
        .map(|entity| {
            PersistedSchemaMigrationEntity::try_new(
                TargetStoreIdentity::from_bytes([0x96; 32]),
                EntityTag::new(
                    first_entity
                        .value()
                        .checked_add(entity - 1)
                        .ok_or_else(InternalError::store_invariant)?,
                ),
                EntitySourceDigest::from_bytes([0x97; 32]),
            )
        })
        .collect::<Result<_, _>>()?;
    record.staged_indexes = (1..=63)
        .map(|index| PersistedSchemaMigrationIndex {
            store: TargetStoreIdentity::from_bytes([0x96; 32]),
            entity: first_entity,
            index,
            generation: 7,
        })
        .collect();
    record.transitions = (1..=entity_count)
        .map(|entity| {
            PersistedSchemaMigrationTransition::try_new(
                EntitySourceKey::try_new(format!("RecoveryEntity{entity:05}"))
                    .map_err(|_| InternalError::store_invariant())?,
                1,
                2,
            )
        })
        .collect::<Result<_, _>>()?;
    let mut operations = vec![SchemaMigrationRecordOp::insert(&record)?];
    for phase in [
        PersistedSchemaMigrationPhase::Validating,
        PersistedSchemaMigrationPhase::ReadyToRewrite,
        PersistedSchemaMigrationPhase::RewritingRows,
    ] {
        let next = record.transition(phase, PersistedSchemaMigrationProgress::default())?;
        operations.push(SchemaMigrationRecordOp::replace(&record, &next)?);
        record = next;
    }

    Ok(operations)
}
