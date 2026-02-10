use icydb_core::traits::{
    AsView as CoreAsView, CreateView as CoreCreateView, UpdateView as CoreUpdateView,
};

///
/// View
///
/// Canonical projected view type for `T`.
///

pub type View<T> = <T as CoreAsView>::ViewType;

///
/// Create
///
/// Create payload type for `T`.
///

pub type Create<T> = <T as CoreCreateView>::CreateViewType;

///
/// Update
///
/// Update payload type for `T`.
///

pub type Update<T> = <T as CoreUpdateView>::UpdateViewType;
