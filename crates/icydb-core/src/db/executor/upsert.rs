use crate::{
    db::{
        Db, ensure_recovered,
        executor::{ExecutorError, SaveExecutor, resolve_unique_pk},
        store::DataKey,
        traits::FromKey,
    },
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
    sanitize::sanitize,
    traits::EntityKind,
};
use std::marker::PhantomData;

///
/// UniqueIndexHandle
/// Validated handle to a unique index for an entity type.
///

#[derive(Clone, Copy)]
pub struct UniqueIndexHandle {
    index: &'static IndexModel,
}

impl UniqueIndexHandle {
    #[must_use]
    /// Return the underlying index specification.
    pub const fn index(&self) -> &'static IndexModel {
        self.index
    }

    /// Wrap a unique index for the given entity type.
    pub fn new<E: EntityKind>(index: &'static IndexModel) -> Result<Self, InternalError> {
        if !E::INDEXES.iter().any(|cand| **cand == *index) {
            return Err(
                ExecutorError::IndexNotFound(E::PATH.to_string(), index.fields.join(", ")).into(),
            );
        }

        if !index.unique {
            return Err(ExecutorError::IndexNotUnique(
                E::PATH.to_string(),
                index.fields.join(", "),
            )
            .into());
        }

        Ok(Self { index })
    }

    /// Resolve a unique index by its field list for the given entity type.
    pub fn for_fields<E: EntityKind>(fields: &[&str]) -> Result<Self, InternalError> {
        for index in E::INDEXES {
            if index.fields == fields {
                return Self::new::<E>(index);
            }
        }

        Err(ExecutorError::IndexNotFound(E::PATH.to_string(), fields.join(", ")).into())
    }
}

///
/// UpsertResult
///

/// Result of an upsert that reports whether the entity was inserted.
pub struct UpsertResult<E> {
    pub entity: E,
    pub inserted: bool,
}

///
/// UpsertExecutor
///

#[derive(Clone, Copy)]
pub struct UpsertExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> UpsertExecutor<E>
where
    E::PrimaryKey: FromKey,
{
    /// Construct a new upsert executor.
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    /// Enable debug logging for subsequent upsert operations.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("{}", s.into());
        }
    }

    /// Upsert using a unique index specification.
    pub fn by_unique_index(&self, index: UniqueIndexHandle, entity: E) -> Result<E, InternalError> {
        self.upsert(index.index(), entity)
    }

    /// Upsert using a unique index specification with a merge closure.
    pub fn by_unique_index_merge<F>(
        &self,
        index: UniqueIndexHandle,
        entity: E,
        merge: F,
    ) -> Result<E, InternalError>
    where
        F: FnOnce(E, E) -> E,
    {
        Ok(self
            .by_unique_index_merge_result(index, entity, merge)?
            .entity)
    }

    /// Upsert using a unique index specification with a merge closure, returning an insert/update flag.
    pub fn by_unique_index_merge_result<F>(
        &self,
        index: UniqueIndexHandle,
        entity: E,
        merge: F,
    ) -> Result<UpsertResult<E>, InternalError>
    where
        F: FnOnce(E, E) -> E,
    {
        self.upsert_merge_result(index.index(), entity, merge)
    }

    /// Upsert using a unique index specification, returning an insert/update flag.
    pub fn by_unique_index_result(
        &self,
        index: UniqueIndexHandle,
        entity: E,
    ) -> Result<UpsertResult<E>, InternalError> {
        self.upsert_result(index.index(), entity)
    }

    /// Upsert using a unique index identified by its field list.
    pub fn by_unique_fields(&self, fields: &[&str], entity: E) -> Result<E, InternalError> {
        let index = UniqueIndexHandle::for_fields::<E>(fields)?;
        self.upsert(index.index(), entity)
    }

    /// Upsert using a unique index identified by its field list with a merge closure.
    pub fn by_unique_fields_merge<F>(
        &self,
        fields: &[&str],
        entity: E,
        merge: F,
    ) -> Result<E, InternalError>
    where
        F: FnOnce(E, E) -> E,
    {
        Ok(self
            .by_unique_fields_merge_result(fields, entity, merge)?
            .entity)
    }

    /// Upsert using a unique index identified by its field list with a merge closure, returning an insert/update flag.
    pub fn by_unique_fields_merge_result<F>(
        &self,
        fields: &[&str],
        entity: E,
        merge: F,
    ) -> Result<UpsertResult<E>, InternalError>
    where
        F: FnOnce(E, E) -> E,
    {
        let index = UniqueIndexHandle::for_fields::<E>(fields)?;
        self.upsert_merge_result(index.index(), entity, merge)
    }

    /// Upsert using a unique index identified by its field list, returning an insert/update flag.
    pub fn by_unique_fields_result(
        &self,
        fields: &[&str],
        entity: E,
    ) -> Result<UpsertResult<E>, InternalError> {
        let index = UniqueIndexHandle::for_fields::<E>(fields)?;
        self.upsert_result(index.index(), entity)
    }

    ///
    /// --------------------------------- PRIVATE METHODS ------------------------------------------------
    ///

    /// Compute the lookup entity (sanitized) and resolve the existing pk for the given unique index.
    ///
    /// We sanitize the lookup copy to ensure the index key is derived from the canonical (sanitized)
    /// representation of the unique fields.
    fn resolve_existing_pk(
        &self,
        index: &'static IndexModel,
        entity: &E,
    ) -> Result<Option<E::PrimaryKey>, InternalError> {
        let mut lookup = entity.clone();
        sanitize(&mut lookup)?;
        resolve_unique_pk::<E>(&self.db, index, &lookup)
    }

    fn upsert(&self, index: &'static IndexModel, entity: E) -> Result<E, InternalError> {
        Ok(self.upsert_result(index, entity)?.entity)
    }

    fn upsert_result(
        &self,
        index: &'static IndexModel,
        entity: E,
    ) -> Result<UpsertResult<E>, InternalError> {
        self.debug_log(format!(
            "[debug] upsert on {} (unique index: {})",
            E::PATH,
            index.fields.join(", ")
        ));
        // Recovery is mutation-only to keep read paths side-effect free.
        ensure_recovered(&self.db)?;
        let existing_pk = self.resolve_existing_pk(index, &entity)?;
        let inserted = existing_pk.is_none();

        // Keep saver construction local to avoid type/lifetime issues in helpers.
        let saver = SaveExecutor::new(self.db, self.debug);

        let entity = match existing_pk {
            Some(pk) => {
                let mut entity = entity;
                entity.set_primary_key(pk);
                saver.update(entity)?
            }
            None => saver.insert(entity)?,
        };

        Ok(UpsertResult { entity, inserted })
    }

    fn upsert_merge_result<F>(
        &self,
        index: &'static IndexModel,
        entity: E,
        merge: F,
    ) -> Result<UpsertResult<E>, InternalError>
    where
        F: FnOnce(E, E) -> E,
    {
        self.debug_log(format!(
            "[debug] upsert merge on {} (unique index: {})",
            E::PATH,
            index.fields.join(", ")
        ));
        // Recovery is mutation-only to keep read paths side-effect free.
        ensure_recovered(&self.db)?;
        let existing_pk = self.resolve_existing_pk(index, &entity)?;

        // Keep saver construction local to avoid type/lifetime issues in helpers.
        let saver = SaveExecutor::new(self.db, self.debug);

        let result = if let Some(pk) = existing_pk {
            // Load existing entity by pk and merge caller's entity into it.
            let existing = self.load_existing(index, pk)?;
            let mut merged = merge(existing, entity);
            merged.set_primary_key(pk);

            let entity = saver.update(merged)?;
            UpsertResult {
                entity,
                inserted: false,
            }
        } else {
            let entity = saver.insert(entity)?;
            UpsertResult {
                entity,
                inserted: true,
            }
        };

        Ok(result)
    }

    fn load_existing(
        &self,
        index: &'static IndexModel,
        pk: E::PrimaryKey,
    ) -> Result<E, InternalError> {
        let data_key = DataKey::new::<E>(pk.into());
        let raw_data_key = data_key.to_raw();
        let row = self
            .db
            .context::<E>()
            .with_store(|store| store.get(&raw_data_key))?;

        let Some(row) = row else {
            // Index pointed at a key that does not exist in the primary store.
            return Err(ExecutorError::corruption(
                ErrorOrigin::Index,
                format!(
                    "index corrupted: {} ({}) -> {} keys",
                    E::PATH,
                    index.fields.join(", "),
                    1
                ),
            )
            .into());
        };

        row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key} ({err})"),
            )
            .into()
        })
    }
}
