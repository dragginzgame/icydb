//! Module: db::schema::inspection_plan
//! Responsibility: canonical accepted-schema projection for integrity inspection.
//! Does not own: physical traversal, inspection progress, or diagnostic rendering.
//! Boundary: binds one verified accepted entity snapshot and value catalog to
//! the row-program authority and fingerprint consumed by Quick and Deep inspection.

use crate::{
    db::{
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u32,
            write_hash_u64,
        },
        data::StructuralRowContract,
        index::AcceptedIndexInspectionPlan,
        schema::{
            AcceptedCatalogIdentity, AcceptedRowLayoutRuntimeContract, AcceptedSchemaAuthority,
            AcceptedSchemaFingerprint, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            CompiledAcceptedRowConstraints,
        },
    },
    error::InternalError,
};
use sha2::Digest;

const ACCEPTED_INSPECTION_PLAN_FINGERPRINT_DOMAIN: &[u8] = b"icydb.accepted-inspection-plan.v1";

/// Semantic fingerprint of one accepted inspection plan.
///
/// The fingerprint binds the selected entity schema, its accepted store-local
/// value catalog, and the inspection semantics version. It is not a second
/// schema authority: the selected accepted snapshot and root fingerprints are
/// its inputs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct AcceptedInspectionPlanFingerprint([u8; 32]);

impl AcceptedInspectionPlanFingerprint {
    /// Return the canonical fingerprint bytes.
    #[must_use]
    pub(in crate::db) const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Bounded accepted-native input to integrity inspection.
///
/// This artifact carries the exact accepted entity snapshot, catalog authority,
/// and precompiled row program already used by write admission. Later
/// inspection phases add physical traversal contracts to this owner rather
/// than rebuilding schema meaning from generated models.
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedInspectionPlan {
    identity: AcceptedCatalogIdentity,
    snapshot: AcceptedSchemaSnapshot,
    value_catalog: AcceptedValueCatalogHandle,
    row_contract: StructuralRowContract,
    write_constraints: CompiledAcceptedRowConstraints,
    index_inspection: AcceptedIndexInspectionPlan,
    fingerprint: AcceptedInspectionPlanFingerprint,
}

impl AcceptedInspectionPlan {
    /// Build one plan from a verified accepted selection.
    pub(in crate::db) fn compile(
        identity: AcceptedCatalogIdentity,
        snapshot: AcceptedSchemaSnapshot,
        value_catalog: AcceptedValueCatalogHandle,
    ) -> Result<Self, InternalError> {
        if value_catalog.revision() != identity.accepted_schema_revision() {
            return Err(InternalError::store_invariant());
        }

        let accepted_schema_fingerprint = identity.accepted_schema_fingerprint();
        let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&snapshot)?;
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            identity.entity_path(),
            row_layout.row_decode_contract(value_catalog.clone()),
        );
        let write_constraints = CompiledAcceptedRowConstraints::compile(
            &snapshot,
            &value_catalog,
            accepted_schema_fingerprint,
        )
        .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?;
        let index_inspection =
            AcceptedIndexInspectionPlan::compile(&snapshot, value_catalog.clone(), &row_contract)?;
        let fingerprint =
            accepted_inspection_plan_fingerprint(identity, value_catalog.authority().fingerprint());

        Ok(Self {
            identity,
            snapshot,
            value_catalog,
            row_contract,
            write_constraints,
            index_inspection,
            fingerprint,
        })
    }

    /// Return the selected accepted catalog identity.
    #[must_use]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity
    }

    /// Return whether this plan still matches one selected accepted authority.
    #[must_use]
    pub(in crate::db) fn matches_selection(
        &self,
        identity: AcceptedCatalogIdentity,
        authority: &AcceptedSchemaAuthority,
    ) -> bool {
        self.identity == identity
            && self.value_catalog.authority() == authority
            && self.fingerprint
                == accepted_inspection_plan_fingerprint(identity, authority.fingerprint())
    }

    /// Borrow the selected accepted entity snapshot.
    #[must_use]
    pub(in crate::db) const fn snapshot(&self) -> &AcceptedSchemaSnapshot {
        &self.snapshot
    }

    /// Borrow the selected store-local value catalog.
    #[must_use]
    pub(in crate::db) const fn value_catalog(&self) -> &AcceptedValueCatalogHandle {
        &self.value_catalog
    }

    /// Borrow the accepted current/historical structural row contract.
    #[must_use]
    pub(in crate::db) const fn row_contract(&self) -> &StructuralRowContract {
        &self.row_contract
    }

    /// Borrow the write-admission row program for this accepted identity.
    #[must_use]
    pub(in crate::db) const fn write_constraints(&self) -> &CompiledAcceptedRowConstraints {
        &self.write_constraints
    }

    /// Borrow precompiled active forward-index witness authority.
    #[must_use]
    pub(in crate::db) const fn index_inspection(&self) -> &AcceptedIndexInspectionPlan {
        &self.index_inspection
    }

    /// Return the fingerprint of the complete accepted inspection projection.
    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> AcceptedInspectionPlanFingerprint {
        self.fingerprint
    }
}

fn accepted_inspection_plan_fingerprint(
    identity: AcceptedCatalogIdentity,
    accepted_root_fingerprint: AcceptedSchemaFingerprint,
) -> AcceptedInspectionPlanFingerprint {
    let mut hasher = new_hash_sha256_prefixed(ACCEPTED_INSPECTION_PLAN_FINGERPRINT_DOMAIN);
    write_hash_u64(&mut hasher, identity.entity_tag().value());
    write_hash_str_u32(&mut hasher, identity.entity_path());
    write_hash_str_u32(&mut hasher, identity.store_path());
    write_hash_u64(&mut hasher, identity.accepted_schema_revision().get());
    write_hash_u32(&mut hasher, identity.accepted_schema_version().get());
    hasher.update([identity.fingerprint_method_version()]);
    hasher.update(identity.accepted_schema_fingerprint());
    hasher.update(accepted_root_fingerprint.as_bytes());

    AcceptedInspectionPlanFingerprint(finalize_hash_sha256(hasher))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            commit::CommitSchemaFingerprint,
            schema::{
                AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision, FieldId,
                PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
                SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
                enum_catalog::build_initial_accepted_enum_catalog,
            },
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
    };

    fn identity(
        revision: AcceptedSchemaRevision,
        fingerprint: CommitSchemaFingerprint,
    ) -> AcceptedCatalogIdentity {
        AcceptedCatalogIdentity::new(
            EntityTag::new(17),
            "tests::InspectionEntity",
            "tests::InspectionStore",
            revision,
            SchemaVersion::initial(),
            fingerprint,
        )
    }

    fn value_catalog(revision: AcceptedSchemaRevision) -> AcceptedValueCatalogHandle {
        AcceptedValueCatalogHandle::new_for_tests(
            build_initial_accepted_enum_catalog(&[])
                .expect("empty accepted enum catalog should build"),
            AcceptedCompositeCatalog::empty(),
            revision,
        )
    }

    fn snapshot() -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::InspectionEntity".to_string(),
            "InspectionEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
        ))
    }

    #[test]
    fn accepted_inspection_plan_compiles_the_write_admission_program_once() {
        let revision = AcceptedSchemaRevision::INITIAL;
        let identity = identity(revision, [0x11; 16]);

        let plan = AcceptedInspectionPlan::compile(identity, snapshot(), value_catalog(revision))
            .expect("verified accepted inputs should compile one inspection plan");

        assert_eq!(plan.identity(), identity);
        assert!(plan.write_constraints().is_empty());
        assert_ne!(plan.fingerprint().to_bytes(), [0; 32]);
    }

    #[test]
    fn accepted_inspection_plan_fingerprint_binds_schema_and_root_identity() {
        let revision = AcceptedSchemaRevision::INITIAL;
        let baseline = accepted_inspection_plan_fingerprint(identity(revision, [0x11; 16]), {
            AcceptedSchemaFingerprint::new([0x22; 32])
        });

        assert_eq!(
            baseline,
            accepted_inspection_plan_fingerprint(identity(revision, [0x11; 16]), {
                AcceptedSchemaFingerprint::new([0x22; 32])
            }),
        );
        assert_ne!(
            baseline,
            accepted_inspection_plan_fingerprint(identity(revision, [0x33; 16]), {
                AcceptedSchemaFingerprint::new([0x22; 32])
            }),
        );
        assert_ne!(
            baseline,
            accepted_inspection_plan_fingerprint(identity(revision, [0x11; 16]), {
                AcceptedSchemaFingerprint::new([0x44; 32])
            }),
        );
    }

    #[test]
    fn accepted_inspection_plan_rejects_mismatched_catalog_revision() {
        let error = AcceptedInspectionPlan::compile(
            identity(AcceptedSchemaRevision::INITIAL, [0x11; 16]),
            snapshot(),
            value_catalog(AcceptedSchemaRevision::new(2)),
        )
        .expect_err("a plan must not combine different accepted revisions");

        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        );
    }
}
