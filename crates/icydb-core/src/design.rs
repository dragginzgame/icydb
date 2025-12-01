///
/// Design Prelude
///
/// Helpers for schema/design code (derive macros, traits, and types).
///

pub mod prelude {
    pub use ::candid::CandidType;
    pub use derive_more;
    pub use icydb_error::{ErrorTree, err};
    pub use icydb_macros::*;

    pub use crate::{
        Key, Value, db,
        db::Db,
        traits::{
            EntityKind, FieldValue as _, Inner as _, Path as _, Sanitize as _, Sanitizer as _,
            Serialize as _, Validate as _, ValidateCustom, Validator as _, View as _,
            Visitable as _,
        },
        types::*,
        view::View,
    };
}
