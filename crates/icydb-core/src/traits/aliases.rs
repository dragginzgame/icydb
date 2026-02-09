use crate::traits::{AsView, CreateView, UpdateView};

///
/// View
///
/// Canonical projected view type for `T`.
///

pub type View<T> = <T as AsView>::ViewType;

///
/// Create
///
/// Create payload type for `T`.
///

pub type Create<T> = <T as CreateView>::CreateViewType;

///
/// Update
///
/// Update payload type for `T`.
///

pub type Update<T> = <T as UpdateView>::UpdateViewType;
