//! Module: db::schema_evolution::descriptor
//! Responsibility: high-level schema migration descriptors and canonical row-op inputs.
//! Does not own: migration execution or durable migration-progress storage.
//! Boundary: caller-declared schema intent plus validated model identity.

use crate::{
    db::{
        identity::{EntityName, IndexName},
        schema::commit_schema_fingerprint_for_model,
    },
    error::InternalError,
    model::EntityModel,
    traits::EntityKind,
};

///
/// SchemaMigrationEntityTarget
///
/// SchemaMigrationEntityTarget binds one canonical entity identity to the
/// runtime model/path authority needed when schema evolution eventually emits
/// row-level migration operations.
///

#[derive(Clone, Copy, Debug)]
pub struct SchemaMigrationEntityTarget {
    name: EntityName,
    model: &'static EntityModel,
}

impl SchemaMigrationEntityTarget {
    /// Build one schema-evolution target from a generated entity type.
    pub fn for_entity<E>() -> Result<Self, InternalError>
    where
        E: EntityKind + 'static,
    {
        Self::from_model(E::MODEL)
    }

    /// Build one schema-evolution target from a runtime entity model.
    pub fn from_model(model: &'static EntityModel) -> Result<Self, InternalError> {
        let name = EntityName::try_from_str(model.name()).map_err(|err| {
            InternalError::schema_evolution_invalid_identity(format!(
                "invalid entity name '{}': {err}",
                model.name()
            ))
        })?;

        Ok(Self { name, model })
    }

    /// Return the canonical entity identity.
    #[must_use]
    pub const fn name(self) -> EntityName {
        self.name
    }

    /// Return the runtime model that owns this schema-evolution target.
    #[must_use]
    pub const fn model(self) -> &'static EntityModel {
        self.model
    }

    /// Return the runtime entity path consumed by commit runtime hooks.
    #[must_use]
    pub const fn runtime_path(self) -> &'static str {
        self.model.path()
    }

    /// Return the current commit schema fingerprint for this target model.
    #[must_use]
    pub fn schema_fingerprint(self) -> [u8; 16] {
        commit_schema_fingerprint_for_model(self.model.path(), self.model)
    }
}

///
/// SchemaMigrationStepIntent
///
/// SchemaMigrationStepIntent describes the high-level schema change that must be
/// validated before any row-op migration plan is emitted.
/// The initial supported slice models an index addition because `IndexName`
/// already provides canonical entity + field identity.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaMigrationStepIntent {
    AddIndex { index: IndexName },
}

impl SchemaMigrationStepIntent {
    /// Build one canonical add-index migration intent.
    #[must_use]
    pub const fn add_index(index: IndexName) -> Self {
        Self::AddIndex { index }
    }
}

///
/// SchemaMigrationRowOp
///
/// SchemaMigrationRowOp is the schema-evolution-owned row rewrite description.
/// It carries a canonical entity target and raw row bytes, then the planner
/// converts it into the lower-level migration row-op DTO only after validation.
///

#[derive(Clone, Debug)]
pub struct SchemaMigrationRowOp {
    target: SchemaMigrationEntityTarget,
    key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
}

impl SchemaMigrationRowOp {
    /// Build one explicit row rewrite for a schema migration.
    #[must_use]
    pub const fn new(
        target: SchemaMigrationEntityTarget,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    ) -> Self {
        Self {
            target,
            key,
            before,
            after,
        }
    }

    /// Build one insert-style row rewrite for a schema migration.
    #[must_use]
    pub const fn insert(target: SchemaMigrationEntityTarget, key: Vec<u8>, after: Vec<u8>) -> Self {
        Self::new(target, key, None, Some(after))
    }

    /// Return the canonical entity target for this row rewrite.
    #[must_use]
    pub const fn target(&self) -> SchemaMigrationEntityTarget {
        self.target
    }

    /// Borrow encoded raw data-key bytes.
    #[must_use]
    pub const fn key(&self) -> &[u8] {
        self.key.as_slice()
    }

    /// Borrow the optional before-image row payload.
    #[must_use]
    pub fn before(&self) -> Option<&[u8]> {
        self.before.as_deref()
    }

    /// Borrow the optional after-image row payload.
    #[must_use]
    pub fn after(&self) -> Option<&[u8]> {
        self.after.as_deref()
    }

    pub(in crate::db) fn into_migration_row_op(
        self,
    ) -> Result<crate::db::migration::MigrationRowOp, InternalError> {
        crate::db::migration::MigrationRowOp::new(
            self.target.runtime_path(),
            self.key,
            self.before,
            self.after,
            self.target.schema_fingerprint(),
        )
    }
}

///
/// SchemaDataTransformation
///
/// SchemaDataTransformation describes the data rewrite portion of one schema
/// migration descriptor.
/// The first slice accepts explicit row rewrites only; derivation engines can
/// add richer variants later without changing `db::migration` execution.
///

#[derive(Clone, Debug)]
pub enum SchemaDataTransformation {
    ExplicitRowOps(Vec<SchemaMigrationRowOp>),
}

impl SchemaDataTransformation {
    /// Build one explicit row-op data transformation.
    #[must_use]
    pub const fn explicit_row_ops(row_ops: Vec<SchemaMigrationRowOp>) -> Self {
        Self::ExplicitRowOps(row_ops)
    }

    /// Borrow the explicit row-op payload for this transformation.
    #[must_use]
    pub const fn row_ops(&self) -> &[SchemaMigrationRowOp] {
        match self {
            Self::ExplicitRowOps(row_ops) => row_ops.as_slice(),
        }
    }

    pub(in crate::db) fn into_row_ops(self) -> Vec<SchemaMigrationRowOp> {
        match self {
            Self::ExplicitRowOps(row_ops) => row_ops,
        }
    }
}

///
/// SchemaMigrationDescriptor
///
/// SchemaMigrationDescriptor is the schema-evolution authority for one
/// high-level migration.
/// It names the migration, freezes the monotonic version, records human-facing
/// description text, and carries validated schema/data intent for planning.
///

#[derive(Clone, Debug)]
pub struct SchemaMigrationDescriptor {
    migration_id: EntityName,
    version: u64,
    description: String,
    intent: SchemaMigrationStepIntent,
    data_transformation: Option<SchemaDataTransformation>,
}

impl SchemaMigrationDescriptor {
    /// Build one validated schema migration descriptor.
    pub fn new(
        migration_id: EntityName,
        version: u64,
        description: impl Into<String>,
        intent: SchemaMigrationStepIntent,
        data_transformation: Option<SchemaDataTransformation>,
    ) -> Result<Self, InternalError> {
        let description = description.into();
        if version == 0 {
            return Err(InternalError::schema_evolution_version_required(
                migration_id.as_str(),
            ));
        }
        if description.trim().is_empty() {
            return Err(InternalError::schema_evolution_description_required(
                migration_id.as_str(),
            ));
        }

        Ok(Self {
            migration_id,
            version,
            description,
            intent,
            data_transformation,
        })
    }

    /// Return the canonical migration identity.
    #[must_use]
    pub const fn migration_id(&self) -> EntityName {
        self.migration_id
    }

    /// Return the monotonic schema migration version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Borrow the descriptor description.
    #[must_use]
    pub const fn description(&self) -> &str {
        self.description.as_str()
    }

    /// Borrow the high-level schema change intent.
    #[must_use]
    pub const fn intent(&self) -> &SchemaMigrationStepIntent {
        &self.intent
    }

    /// Borrow the optional data transformation.
    #[must_use]
    pub const fn data_transformation(&self) -> Option<&SchemaDataTransformation> {
        self.data_transformation.as_ref()
    }

    pub(in crate::db) fn into_data_transformation(self) -> Option<SchemaDataTransformation> {
        self.data_transformation
    }
}
