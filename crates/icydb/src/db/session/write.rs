//! Module: db::session::write
//!
//! Responsibility: public `DbSession` write helpers, write-returning projection
//! conversion, and structural mutation facade types.
//! Does not own: core mutation execution, commit staging, or persisted encoding.
//! Boundary: keeps public write semantics and row-returning projection payloads
//! above the core save pipeline.

use crate::{
    ErrorCode,
    db::{DynamicMutationResult, response::RowProjectionOutput, session::DbSession},
    diagnostic::RuntimeBoundaryCode,
    error::{Error, ErrorOrigin},
    traits::CanisterKind,
    value::{InputValue, OutputValue},
};

use icydb_core as core;
use std::{error::Error as StdError, fmt};

///
/// WriteCell
///
/// Explicit authored intent for one structural or generated typed write field.
/// The database retains the distinction through accepted-policy resolution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteCell<T> {
    /// Supply no authored value for this operation.
    Omitted,
    /// Explicitly request the accepted database default.
    Default,
    /// Explicitly author `NULL`.
    Null,
    /// Explicitly author one concrete value.
    Value(T),
}

/// One complete accepted row supplied to an opted-in generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRow {
    entity: String,
    columns: Vec<String>,
    values: Vec<OutputValue>,
}

impl OutputRow {
    /// Build one accepted row projection.
    pub fn new(
        entity: impl Into<String>,
        columns: Vec<String>,
        values: Vec<OutputValue>,
    ) -> Result<Self, TypedAdapterError> {
        if columns.len() != values.len() {
            return Err(TypedAdapterError::RowShapeMismatch);
        }
        Ok(Self {
            entity: entity.into(),
            columns,
            values,
        })
    }

    /// Borrow the accepted entity display name.
    #[must_use]
    pub const fn entity(&self) -> &str {
        self.entity.as_str()
    }
}

/// Stable typed-adapter boundary failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedAdapterError {
    /// The binding no longer matches current accepted authority.
    StaleBinding,
    /// The row belongs to another accepted entity.
    EntityMismatch,
    /// An immutable source key is absent from the binding projection.
    FieldUnavailable,
    /// The row does not contain the bound accepted field.
    RowFieldUnavailable,
    /// Column and value cardinalities disagree.
    RowShapeMismatch,
}

impl fmt::Display for TypedAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StaleBinding => "typed binding is stale",
            Self::EntityMismatch => "typed binding entity mismatch",
            Self::FieldUnavailable => "typed binding field unavailable",
            Self::RowFieldUnavailable => "typed row field unavailable",
            Self::RowShapeMismatch => "typed row shape mismatch",
        })
    }
}

impl StdError for TypedAdapterError {}

/// Failure while validating or executing one typed write.
#[derive(Debug)]
pub enum TypedWriteError {
    /// The opaque adapter binding is stale or mismatched.
    Adapter(TypedAdapterError),
    /// The accepted database write rejected or failed.
    Database(Error),
}

impl fmt::Display for TypedWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedWriteError {}

impl From<Error> for TypedWriteError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}

/// Opaque accepted-schema binding for one opted-in generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedEntityBinding {
    inner: core::db::DynamicTypedEntityBinding,
}

impl TypedEntityBinding {
    const fn new(inner: core::db::DynamicTypedEntityBinding) -> Self {
        Self { inner }
    }

    /// Borrow one bound row value by immutable field source key.
    pub fn row_value<'a>(
        &self,
        field_source_key: &str,
        row: &'a OutputRow,
    ) -> Result<&'a OutputValue, TypedAdapterError> {
        if row.entity != self.inner.entity() {
            return Err(TypedAdapterError::EntityMismatch);
        }
        let field = self
            .inner
            .field_name(field_source_key)
            .ok_or(TypedAdapterError::FieldUnavailable)?;
        let index = row
            .columns
            .iter()
            .position(|column| column == field)
            .ok_or(TypedAdapterError::RowFieldUnavailable)?;
        row.values
            .get(index)
            .ok_or(TypedAdapterError::RowShapeMismatch)
    }
}

/// IcyDB-owned decode adapter implemented only by opted-in generated code.
pub trait TypedRowAdapter {
    /// Complete application row produced by decoding.
    type Row;

    /// Decode one accepted output row through an opaque current binding.
    fn decode_row(
        binding: &TypedEntityBinding,
        row: OutputRow,
    ) -> Result<Self::Row, TypedAdapterError>;
}

/// IcyDB-owned write adapter implemented only by opted-in generated inputs.
pub trait TypedWriteAdapter {
    /// Lower explicit application write intent without resolving database policy.
    fn encode_write(self, binding: &TypedEntityBinding) -> Result<TypedWrite, TypedAdapterError>;
}

/// One generated write lowered through immutable source keys.
#[derive(Clone, Debug)]
pub struct TypedWrite {
    binding: TypedEntityBinding,
    mutation: StructuralMutation,
}

impl TypedWrite {
    /// Build one insert intent from immutable field source keys.
    pub fn insert<I, S>(binding: &TypedEntityBinding, fields: I) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = structural_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: StructuralMutation::Insert {
                entity: binding.inner.entity().to_string(),
                patch,
            },
        })
    }

    /// Build one patch/update intent from immutable field source keys.
    pub fn update<I, S>(
        binding: &TypedEntityBinding,
        key: InputValue,
        fields: I,
    ) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = structural_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: StructuralMutation::Update {
                entity: binding.inner.entity().to_string(),
                key,
                patch,
            },
        })
    }

    /// Build one replacement intent from immutable field source keys.
    pub fn replace<I, S>(
        binding: &TypedEntityBinding,
        key: InputValue,
        fields: I,
    ) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = structural_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: StructuralMutation::Replace {
                entity: binding.inner.entity().to_string(),
                key,
                patch,
            },
        })
    }
}

fn structural_patch_from_binding<I, S>(
    binding: &TypedEntityBinding,
    fields: I,
) -> Result<StructuralPatch, TypedAdapterError>
where
    I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
    S: AsRef<str>,
{
    let mut patch = StructuralPatch::new();
    for (source, cell) in fields {
        let field = binding
            .inner
            .field_name(source.as_ref())
            .ok_or(TypedAdapterError::FieldUnavailable)?;
        patch = patch.field(field, cell);
    }
    Ok(patch)
}

impl WriteCell<InputValue> {
    fn into_core(self) -> core::db::DynamicWriteCell {
        match self {
            Self::Omitted => core::db::DynamicWriteCell::Omitted,
            Self::Default => core::db::DynamicWriteCell::Default,
            Self::Null => core::db::DynamicWriteCell::Null,
            Self::Value(value) => core::db::DynamicWriteCell::Value(value),
        }
    }
}

///
/// StructuralPatch
///
/// Public field-name-driven structural mutation patch.
/// Names are resolved only when the request is admitted against accepted
/// schema; the patch cannot carry physical row slots or generated field order.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StructuralPatch {
    fields: Vec<(String, WriteCell<InputValue>)>,
}

impl StructuralPatch {
    /// Build one empty structural patch.
    #[must_use]
    pub const fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Append one named field intent.
    #[must_use]
    pub fn field(mut self, name: impl Into<String>, value: WriteCell<InputValue>) -> Self {
        self.fields.push((name.into(), value));
        self
    }

    fn into_core(self) -> core::db::DynamicStructuralPatch {
        core::db::DynamicStructuralPatch::new(
            self.fields
                .into_iter()
                .map(|(name, value)| (name, value.into_core()))
                .collect(),
        )
    }
}

///
/// StructuralMutation
///
/// Entity-name-driven dynamic mutation request.
/// Each variant owns its key requirement and row-existence semantics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StructuralMutation {
    /// Insert one row and derive its identity from the accepted after-image.
    Insert {
        /// Accepted entity display name.
        entity: String,
        /// Authored insert fields.
        patch: StructuralPatch,
    },
    /// Patch one existing row.
    Update {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
        /// Authored update fields.
        patch: StructuralPatch,
    },
    /// Replace one row, inserting it when absent.
    Replace {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
        /// Authored replacement fields.
        patch: StructuralPatch,
    },
    /// Delete one existing row.
    Delete {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
    },
}

impl StructuralMutation {
    fn into_core(self) -> core::db::DynamicMutation {
        match self {
            Self::Insert { entity, patch } => core::db::DynamicMutation::Insert {
                entity,
                patch: patch.into_core(),
            },
            Self::Update { entity, key, patch } => core::db::DynamicMutation::Update {
                entity,
                key,
                patch: patch.into_core(),
            },
            Self::Replace { entity, key, patch } => core::db::DynamicMutation::Replace {
                entity,
                key,
                patch: patch.into_core(),
            },
            Self::Delete { entity, key } => core::db::DynamicMutation::Delete { entity, key },
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn projection_selection<E>(
        selected_fields: Option<&[String]>,
    ) -> Result<(Vec<String>, Vec<usize>), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        match selected_fields {
            None => Ok((
                E::MODEL
                    .fields()
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect(),
                (0..E::MODEL.fields().len()).collect(),
            )),
            Some(fields) => {
                let mut indices = Vec::with_capacity(fields.len());

                for field in fields {
                    let index = E::MODEL
                        .fields()
                        .iter()
                        .position(|candidate| candidate.name() == field.as_str())
                        .ok_or_else(|| {
                            Error::from_runtime_boundary(
                                RuntimeBoundaryCode::RowProjectionFieldNotConfigured,
                                ErrorOrigin::Query,
                            )
                        })?;
                    indices.push(index);
                }

                Ok((fields.to_vec(), indices))
            }
        }
    }

    pub(crate) fn row_projection_output_from_entities<E>(
        entity_name: String,
        entities: Vec<E>,
        selected_fields: Option<&[String]>,
        mut project: impl FnMut(&E, &[usize]) -> Result<Vec<OutputValue>, Error>,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        // Phase 1: resolve the explicit outward projection contract before
        // rendering any row data so every row-producing typed write helper
        // shares one field-selection rule.
        let (columns, indices) = Self::projection_selection::<E>(selected_fields)?;
        let mut rows = Vec::with_capacity(entities.len());

        // Phase 2: move selected entity slots into the typed output payload so
        // row-producing write surfaces do not pre-render blob fields as text.
        for entity in entities {
            rows.push(project(&entity, indices.as_slice())?);
        }

        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(RowProjectionOutput {
            entity: entity_name,
            columns,
            rows,
            row_count,
        })
    }

    fn returning_fields<I, S>(fields: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        fields
            .into_iter()
            .map(|field| field.as_ref().to_string())
            .collect()
    }

    fn row_projection_output_from_entity<E>(
        &self,
        entity: E,
        selected_fields: Option<&[String]>,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Self::row_projection_output_from_entities::<E>(
            E::PATH.to_string(),
            vec![entity],
            selected_fields,
            |entity, slots| {
                self.inner
                    .project_entity_output_values(entity, slots)
                    .map_err(|_| {
                        Error::from_error_code(ErrorCode::RUNTIME_INTERNAL, ErrorOrigin::Query)
                    })
            },
        )
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert(entity)?)
    }

    /// Insert one full entity and return every persisted field.
    pub fn insert_returning_all<E>(&self, entity: E) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.insert(entity)?;

        self.row_projection_output_from_entity::<E>(entity, None)
    }

    /// Insert one full entity and return one explicit field list.
    pub fn insert_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.insert(entity)?;
        let fields = Self::returning_fields(fields);

        self.row_projection_output_from_entity::<E>(entity, Some(fields.as_slice()))
    }

    /// Create one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<I::Entity, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.create(input)?)
    }

    /// Create one authored typed input and return every persisted field.
    pub fn create_returning_all<I>(&self, input: I) -> Result<RowProjectionOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.create(input)?;

        self.row_projection_output_from_entity::<I::Entity>(entity, None)
    }

    /// Create one authored typed input and return one explicit field list.
    pub fn create_returning<I, F, S>(
        &self,
        input: I,
        fields: F,
    ) -> Result<RowProjectionOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
        F: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.create(input)?;
        let fields = Self::returning_fields(fields);

        self.row_projection_output_from_entity::<I::Entity>(entity, Some(fields.as_slice()))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert_many_atomic(entities)?.entities())
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::insert_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert_many_non_atomic(entities)?.entities())
    }

    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace(entity)?)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace_many_atomic(entities)?.entities())
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::replace_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace_many_non_atomic(entities)?.entities())
    }

    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update(entity)?)
    }

    /// Update one full entity and return every persisted field.
    pub fn update_returning_all<E>(&self, entity: E) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.update(entity)?;

        self.row_projection_output_from_entity::<E>(entity, None)
    }

    /// Update one full entity and return one explicit field list.
    pub fn update_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.update(entity)?;
        let fields = Self::returning_fields(fields);

        self.row_projection_output_from_entity::<E>(entity, Some(fields.as_slice()))
    }

    /// Execute one trusted structural mutation through accepted schema only.
    ///
    /// This dynamic lane never materializes a generated entity and never runs
    /// application validators or normalizers. The caller must enforce any
    /// required authorization before dispatch.
    pub fn execute_trusted_structural_mutation(
        &self,
        request: StructuralMutation,
    ) -> Result<DynamicMutationResult, Error> {
        Ok(self
            .inner
            .execute_trusted_dynamic_mutation(&request.into_core())?)
    }

    /// Issue one opaque current accepted binding for generated immutable source keys.
    pub fn bind_typed_entity<I, S>(
        &self,
        entity_source_key: &str,
        field_source_keys: I,
    ) -> Result<TypedEntityBinding, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let fields = field_source_keys
            .into_iter()
            .map(|source| source.as_ref().to_string())
            .collect::<Vec<_>>();
        Ok(TypedEntityBinding::new(
            self.inner
                .issue_typed_entity_binding(entity_source_key, fields.as_slice())?,
        ))
    }

    /// Execute one generated write only while its opaque accepted binding is current.
    pub fn execute_trusted_typed_write(
        &self,
        write: TypedWrite,
    ) -> Result<DynamicMutationResult, TypedWriteError> {
        if !self
            .inner
            .typed_entity_binding_is_current(&write.binding.inner)
            .map_err(Error::from)?
        {
            return Err(TypedWriteError::Adapter(TypedAdapterError::StaleBinding));
        }
        self.execute_trusted_structural_mutation(write.mutation)
            .map_err(TypedWriteError::Database)
    }

    /// Build one field-name-driven structural patch.
    #[must_use]
    pub fn structural_patch<I, S>(&self, fields: I) -> StructuralPatch
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: Into<String>,
    {
        StructuralPatch {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name.into(), value))
                .collect(),
        }
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update_many_atomic(entities)?.entities())
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::update_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update_many_non_atomic(entities)?.entities())
    }
}
