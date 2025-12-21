use crate::{
    Error, IndexSpec, Key,
    db::{
        Db,
        executor::{ExecutorError, SaveExecutor},
        store::{DataKey, IndexKey},
    },
    traits::EntityKind,
};
use std::{any::type_name, marker::PhantomData};

///
/// UniqueIndexSpec
///

#[derive(Clone, Copy)]
pub struct UniqueIndexSpec {
    index: &'static IndexSpec,
}

impl UniqueIndexSpec {
    #[must_use]
    /// Return the underlying index specification.
    pub const fn index(&self) -> &'static IndexSpec {
        self.index
    }

    /// Wrap a unique index for the given entity type.
    pub fn new<E: EntityKind>(index: &'static IndexSpec) -> Result<Self, Error> {
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
    pub fn for_fields<E: EntityKind>(fields: &[&str]) -> Result<Self, Error> {
        for index in E::INDEXES {
            if index.fields == fields {
                return Self::new::<E>(index);
            }
        }

        Err(ExecutorError::IndexNotFound(E::PATH.to_string(), fields.join(", ")).into())
    }
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
    E::PrimaryKey: PrimaryKeyFromKey,
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

    /// Upsert using a unique index specification.
    pub fn by_unique_index(&self, index: UniqueIndexSpec, entity: E) -> Result<E, Error> {
        self.upsert(index.index(), entity)
    }

    /// Upsert a view using a unique index specification.
    pub fn by_unique_index_view(
        &self,
        index: UniqueIndexSpec,
        view: E::ViewType,
    ) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        Ok(self.by_unique_index(index, entity)?.to_view())
    }

    /// Upsert using a unique index identified by its field list.
    pub fn by_unique_fields(&self, fields: &[&str], entity: E) -> Result<E, Error> {
        let index = UniqueIndexSpec::for_fields::<E>(fields)?;
        self.upsert(index.index(), entity)
    }

    /// Upsert a view using a unique index identified by its field list.
    pub fn by_unique_fields_view(
        &self,
        fields: &[&str],
        view: E::ViewType,
    ) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        Ok(self.by_unique_fields(fields, entity)?.to_view())
    }

    fn upsert(&self, index: &'static IndexSpec, mut entity: E) -> Result<E, Error> {
        let existing_pk = self.resolve_unique_pk(index, &entity)?;
        let saver = SaveExecutor::new(self.db, self.debug);

        if let Some(pk) = existing_pk {
            entity.set_primary_key(pk);
            saver.update(entity)
        } else {
            saver.insert(entity)
        }
    }

    fn resolve_unique_pk(
        &self,
        index: &'static IndexSpec,
        entity: &E,
    ) -> Result<Option<E::PrimaryKey>, Error> {
        let Some(index_key) = IndexKey::new(entity, index) else {
            return Err(ExecutorError::IndexKeyMissing(
                E::PATH.to_string(),
                index.fields.join(", "),
            )
            .into());
        };

        let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
        let entry = store.with_borrow(|s| s.get(&index_key));

        let Some(entry) = entry else {
            return Ok(None);
        };

        let len = entry.len();
        if len == 0 {
            return Err(ExecutorError::IndexCorrupted(
                E::PATH.to_string(),
                index.fields.join(", "),
                len,
            )
            .into());
        }

        if len > 1 {
            return Err(ExecutorError::IndexCorrupted(
                E::PATH.to_string(),
                index.fields.join(", "),
                len,
            )
            .into());
        }

        let key = entry.single_key().ok_or_else(|| {
            ExecutorError::IndexCorrupted(E::PATH.to_string(), index.fields.join(", "), len)
        })?;

        let data_key = DataKey::new::<E>(key);
        let exists = self
            .db
            .context::<E>()
            .with_store(|store| store.get(&data_key).is_some())?;
        if !exists {
            return Err(ExecutorError::IndexCorrupted(
                E::PATH.to_string(),
                index.fields.join(", "),
                len,
            )
            .into());
        }

        Ok(Some(E::PrimaryKey::try_from_key(key)?))
    }
}

/// Convert a stored [`Key`] into a concrete primary key type.
pub trait PrimaryKeyFromKey: Copy {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError>;
}

const fn key_kind(key: &Key) -> &'static str {
    match key {
        Key::Account(_) => "Account",
        Key::Int(_) => "Int",
        Key::Principal(_) => "Principal",
        Key::Subaccount(_) => "Subaccount",
        Key::Timestamp(_) => "Timestamp",
        Key::Uint(_) => "Uint",
        Key::Ulid(_) => "Ulid",
        Key::Unit => "Unit",
    }
}

fn key_type_mismatch<T>(key: &Key) -> ExecutorError {
    ExecutorError::KeyTypeMismatch(type_name::<T>().to_string(), key_kind(key).to_string())
}

fn key_out_of_range<T>(value: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::KeyOutOfRange(type_name::<T>().to_string(), value.to_string())
}

macro_rules! impl_pk_from_key_uint {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl PrimaryKeyFromKey for $ty {
                fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
                    match key {
                        Key::Uint(v) => <$ty>::try_from(v).map_err(|_| key_out_of_range::<$ty>(v)),
                        other => Err(key_type_mismatch::<$ty>(&other)),
                    }
                }
            }
        )*
    };
}

macro_rules! impl_pk_from_key_int {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl PrimaryKeyFromKey for $ty {
                fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
                    match key {
                        Key::Int(v) => <$ty>::try_from(v).map_err(|_| key_out_of_range::<$ty>(v)),
                        other => Err(key_type_mismatch::<$ty>(&other)),
                    }
                }
            }
        )*
    };
}

impl PrimaryKeyFromKey for i64 {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Int(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for u64 {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Uint(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for () {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Unit => Ok(()),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Account {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Account(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Principal {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Principal(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Subaccount {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Subaccount(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Timestamp {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Timestamp(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Ulid {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Ulid(v) => Ok(v),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl PrimaryKeyFromKey for crate::types::Unit {
    fn try_from_key(key: Key) -> Result<Self, ExecutorError> {
        match key {
            Key::Unit => Ok(Self),
            other => Err(key_type_mismatch::<Self>(&other)),
        }
    }
}

impl_pk_from_key_uint!(u8, u16, u32);
impl_pk_from_key_int!(i8, i16, i32);
