use crate::{
    key::{Key, RawKey},
    model::field::EntityFieldKind,
    traits::{
        EntityKind, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Account, Principal, Subaccount, Timestamp, Ulid},
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use std::{fmt, marker::PhantomData};

///
/// Ref
///
/// Typed reference to another entity's primary key.
/// Keeps the target entity type in the type system.
/// Intended for relation fields and reference extraction.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Ref<T: EntityKind> {
    raw: RawKey,
    _marker: PhantomData<fn() -> T>,
}

impl<T: EntityKind> Copy for Ref<T> {}

impl<T: EntityKind> PartialEq for Ref<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<T: EntityKind> Eq for Ref<T> {}

impl<T: EntityKind> PartialOrd for Ref<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: EntityKind> Ord for Ref<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl<T: EntityKind> std::hash::Hash for Ref<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        <RawKey as std::hash::Hash>::hash(&self.raw, state);
    }
}

impl<T: EntityKind> Ref<T> {
    /// Construct a new typed reference from a raw key value.
    #[must_use]
    pub fn new(key: impl Into<Key>) -> Self {
        Self::from_raw(key.into())
    }

    pub(crate) const fn from_raw(raw: RawKey) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn raw(self) -> RawKey {
        self.raw
    }

    /// Runtime kind metadata for reference fields.
    pub const KIND: EntityFieldKind = EntityFieldKind::Ref {
        target_path: T::PATH,
        key_kind: &T::MODEL.primary_key.kind,
    };
}

impl<T: EntityKind> Default for Ref<T> {
    fn default() -> Self {
        let raw = default_raw_key(&T::MODEL.primary_key.kind);
        Self {
            raw,
            _marker: PhantomData,
        }
    }
}

impl<T> CandidType for Ref<T>
where
    T: EntityKind,
{
    fn _ty() -> candid::types::Type {
        <RawKey as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.raw.idl_serialize(serializer)
    }
}

impl<T> FieldValue for Ref<T>
where
    T: EntityKind,
{
    fn to_value(&self) -> Value {
        self.raw.to_value()
    }
}

impl<T: EntityKind> View for Ref<T> {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl<T: EntityKind> UpdateView for Ref<T> {
    type UpdateViewType = Self;

    fn merge(&mut self, update: Self::UpdateViewType) {
        *self = update;
    }
}

impl<T> SanitizeAuto for Ref<T> where T: EntityKind {}
impl<T> SanitizeCustom for Ref<T> where T: EntityKind {}
impl<T> ValidateAuto for Ref<T> where T: EntityKind {}
impl<T> ValidateCustom for Ref<T> where T: EntityKind {}
impl<T> Visitable for Ref<T> where T: EntityKind {}

fn default_raw_key(kind: &EntityFieldKind) -> RawKey {
    match kind {
        EntityFieldKind::Account => Key::Account(Account::default()),
        EntityFieldKind::Int => Key::Int(0),
        EntityFieldKind::Principal => Key::Principal(Principal::default()),
        EntityFieldKind::Subaccount => Key::Subaccount(Subaccount::default()),
        EntityFieldKind::Timestamp => Key::Timestamp(Timestamp::default()),
        EntityFieldKind::Uint => Key::Uint(0),
        EntityFieldKind::Ulid => Key::Ulid(Ulid::default()),
        EntityFieldKind::Ref { key_kind, .. } => default_raw_key(key_kind),
        EntityFieldKind::Blob
        | EntityFieldKind::Bool
        | EntityFieldKind::Date
        | EntityFieldKind::Decimal
        | EntityFieldKind::Duration
        | EntityFieldKind::Enum
        | EntityFieldKind::E8s
        | EntityFieldKind::E18s
        | EntityFieldKind::Float32
        | EntityFieldKind::Float64
        | EntityFieldKind::Int128
        | EntityFieldKind::IntBig
        | EntityFieldKind::Text
        | EntityFieldKind::Uint128
        | EntityFieldKind::UintBig
        | EntityFieldKind::List(_)
        | EntityFieldKind::Set(_)
        | EntityFieldKind::Map { .. }
        | EntityFieldKind::Unit
        | EntityFieldKind::Unsupported => Key::Unit,
    }
}

impl<T: EntityKind> fmt::Display for Ref<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.fmt(f)
    }
}
