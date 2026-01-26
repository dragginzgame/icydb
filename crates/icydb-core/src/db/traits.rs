use crate::key::Key;

///
/// FromKey
/// Convert a stored [`Key`] into a concrete type.
/// Returns `None` if the key cannot represent this type.
///

pub trait FromKey: Copy {
    fn try_from_key(key: Key) -> Option<Self>;
}

#[macro_export]
macro_rules! impl_from_key_int {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl $crate::db::traits::FromKey for $ty {
                fn try_from_key(key: $crate::key::Key) -> Option<Self> {
                    match key {
                        $crate::key::Key::Int(v) => Self::try_from(v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! impl_from_key_uint {
    ( $( $ty:ty ),* $(,)? ) => {
        $(
            impl $crate::db::traits::FromKey for $ty {
                fn try_from_key(key: $crate::key::Key) -> Option<Self> {
                    match key {
                        $crate::key::Key::Uint(v) => Self::try_from(v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_from_key_int!(i8, i16, i32, i64);
impl_from_key_uint!(u8, u16, u32, u64);
