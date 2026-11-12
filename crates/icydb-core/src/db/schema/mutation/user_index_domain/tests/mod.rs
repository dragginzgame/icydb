mod empty_domain;

use super::*;
use crate::db::index::IndexKeyKind;

#[test]
fn projection_preserves_unique_conflicts_and_strict_frame_admission() {
    let id = IndexId::new_with_generation(EntityTag::new(7), 0, 0);
    let unique = BTreeSet::from([id]);
    let entry = |primary: u64| StagedUserIndexDomainEntry {
        key: IndexKey::new_from_components_with_primary_key_value(
            &id,
            IndexKeyKind::User,
            &[vec![42]],
            &crate::db::key_taxonomy::PrimaryKeyValue::from(
                crate::db::key_taxonomy::PrimaryKeyComponent::Nat64(primary),
            ),
        )
        .unwrap()
        .to_raw()
        .unwrap(),
        value: IndexEntryValue::presence(),
    };
    assert!(matches!(
        validate_projection(
            &mut [entry(1), entry(1)],
            &unique,
            ProjectionAuthority::CandidateAfter
        ),
        Err(StagedUserIndexDomainError::DuplicateRawKey)
    ));
    assert!(matches!(
        validate_projection(
            &mut [entry(2), entry(1)],
            &unique,
            ProjectionAuthority::AcceptedBefore
        ),
        Err(StagedUserIndexDomainError::DuplicateUniqueKey)
    ));
    assert!(matches!(
        validate_projection(
            &mut [entry(2), entry(1)],
            &unique,
            ProjectionAuthority::CandidateAfter
        ),
        Err(StagedUserIndexDomainError::CandidateUniqueConflict)
    ));
    let mut malformed = entry(1);
    malformed.key = RawIndexStoreKey::from_persisted_bytes(vec![0]);
    assert!(matches!(
        validate_projection(
            &mut [malformed],
            &unique,
            ProjectionAuthority::CandidateAfter
        ),
        Err(StagedUserIndexDomainError::PhysicalKeyDecode)
    ));
}

#[test]
fn stage_budget_rejections_preserve_the_exact_resource() {
    for resource in [
        SchemaTransitionBudgetResource::SourceRows,
        SchemaTransitionBudgetResource::SourceRowBytes,
        SchemaTransitionBudgetResource::ProjectionEntries,
        SchemaTransitionBudgetResource::DeletionKeys,
        SchemaTransitionBudgetResource::ProjectionWorkUnits,
        SchemaTransitionBudgetResource::StagedRawBytes,
    ] {
        let internal = StagedUserIndexDomainError::BudgetExceeded(resource).into_internal_error();
        assert!(matches!(
            internal.detail(),
            Some(crate::error::ErrorDetail::Store(
                crate::error::StoreError::SchemaTransitionBudgetExceeded {
                    resource: actual,
                }
            )) if *actual == resource
        ));
    }
}

#[test]
fn standard_budget_fixes_and_admits_the_design_measurement_point() {
    let design_rows = std::hint::black_box(2_048usize);
    let design_indexes = std::hint::black_box(8usize);
    let design_domain_entries = design_rows * design_indexes;
    let design_projection_entries = design_domain_entries * 2;
    let design_work_units = design_domain_entries * 3;

    assert_eq!(MAX_SCHEMA_PROJECTION_ENTRIES, 131_072);
    assert_eq!(MAX_DELETION_KEYS, 65_536);
    assert_eq!(MAX_SCHEMA_STAGED_RAW_BYTES, 256 * 1024 * 1024);
    assert_eq!(MAX_SCHEMA_PROJECTION_WORK_UNITS, 262_144);
    let mut source = SchemaTransitionSourceBudget::standard();
    for _ in 0..design_rows {
        source
            .consume_source_row(0)
            .expect("the design row count should fit the shared source budget");
    }
    assert!(MAX_SCHEMA_PROJECTION_ENTRIES >= design_projection_entries);
    assert!(MAX_DELETION_KEYS >= design_domain_entries);
    assert!(MAX_SCHEMA_PROJECTION_WORK_UNITS >= design_work_units);
}

#[test]
fn standard_budget_rejects_each_aggregate_dimension_at_its_boundary() {
    let mut entry_count = StagedUserIndexDomainBudget::standard();
    for _ in 0..MAX_SCHEMA_PROJECTION_ENTRIES {
        assert!(entry_count.consume_projection_entry(0).is_ok());
    }
    assert!(matches!(
        entry_count.consume_projection_entry(0),
        Err(StagedUserIndexDomainError::BudgetExceeded(
            SchemaTransitionBudgetResource::ProjectionEntries,
        )),
    ));

    let mut deletion_count = StagedUserIndexDomainBudget::standard();
    for _ in 0..MAX_DELETION_KEYS {
        assert!(deletion_count.consume_deletion_key(0).is_ok());
    }
    assert!(matches!(
        deletion_count.consume_deletion_key(0),
        Err(StagedUserIndexDomainError::BudgetExceeded(
            SchemaTransitionBudgetResource::DeletionKeys,
        )),
    ));

    let mut work = StagedUserIndexDomainBudget::standard();
    for _ in 0..MAX_SCHEMA_PROJECTION_WORK_UNITS {
        assert!(work.consume_projection_work().is_ok());
    }
    assert!(matches!(
        work.consume_projection_work(),
        Err(StagedUserIndexDomainError::BudgetExceeded(
            SchemaTransitionBudgetResource::ProjectionWorkUnits,
        )),
    ));

    let mut staged_bytes = StagedUserIndexDomainBudget::standard();
    assert!(matches!(
        staged_bytes.consume_staged_bytes(MAX_SCHEMA_STAGED_RAW_BYTES + 1),
        Err(StagedUserIndexDomainError::BudgetExceeded(
            SchemaTransitionBudgetResource::StagedRawBytes,
        )),
    ));
}
