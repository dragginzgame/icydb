mod aliases;

pub use aliases::*;

pub use icydb_core::traits::{
    Add, AddAssign, Atomic, CanisterKind, Collection, Debug, Default, Deserialize,
    DeserializeOwned, Div, DivAssign, EntityIdentity, EntityKey, EntityKeyBytes, EntityKind,
    EntityPlacement, EntitySchema, EntityValue, EnumValue, Eq, FieldProjection, FieldValue,
    FieldValueKind, From, Hash, Inner, Kind, MapCollection, Mul, MulAssign, NumCast,
    NumFromPrimitive, NumToPrimitive, Ordering, PartialEq, Path, Rem, Sanitize, SanitizeAuto,
    SanitizeCustom, Sanitizer, Serialize, SingletonEntity, Storable, StoreKind, Sub, SubAssign,
    TypeKind, Validate, ValidateAuto, ValidateCustom, Validator, Visitable,
};
use icydb_core::traits::{
    AsView as CoreAsView, CreateView as CoreCreateView, UpdateView as CoreUpdateView,
};

use crate::error::Error;

///
/// AsView
///
/// Facade-level view projection contract.
///

pub trait AsView: CoreAsView {
    /// Delegate view projection through the facade trait path.
    fn as_view(&self) -> Self::ViewType {
        <Self as CoreAsView>::as_view(self)
    }

    /// Delegate view reconstruction through the facade trait path.
    fn from_view(view: Self::ViewType) -> Self {
        <Self as CoreAsView>::from_view(view)
    }
}

impl<T> AsView for T where T: CoreAsView {}

///
/// CreateView
///
/// Facade-level create payload contract.
///

pub trait CreateView: CoreCreateView {
    /// Build a value from its create payload through the facade trait surface.
    fn from_create_view(view: Self::CreateViewType) -> Self {
        <Self as CoreCreateView>::from_create_view(view)
    }

    /// Build a value from its create payload through the facade trait surface.
    fn create_from_view(view: Self::CreateViewType) -> Self {
        <Self as CoreCreateView>::from_create_view(view)
    }
}

impl<T> CreateView for T where T: CoreCreateView {}

///
/// UpdateView
///
/// Facade-level update payload contract with interface-level error mapping.
///

pub trait UpdateView: CoreUpdateView {
    fn merge(&mut self, patch: Self::UpdateViewType) -> Result<(), Error> {
        <Self as CoreUpdateView>::merge(self, patch).map_err(Error::from_merge_patch_error)
    }
}

impl<T> UpdateView for T where T: CoreUpdateView {}
