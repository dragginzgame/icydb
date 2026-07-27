//! Module: visitor::macros
//! Responsibility: internal macro helpers for repetitive visitor-trait impls.
//! Does not own: public trait definitions or generated type semantics.
//! Boundary: keeps primitive visitor boilerplate local to its owning subsystem.

///
/// MACROS
///

// impl_primitive
macro_rules! impl_primitive {
    ($trait:ident) => {
        impl $trait for i8 {}
        impl $trait for i16 {}
        impl $trait for i32 {}
        impl $trait for i64 {}
        impl $trait for i128 {}
        impl $trait for u8 {}
        impl $trait for u16 {}
        impl $trait for u32 {}
        impl $trait for u64 {}
        impl $trait for u128 {}
        impl $trait for f32 {}
        impl $trait for f64 {}
        impl $trait for bool {}
        impl $trait for String {}
        impl $trait for crate::schema::Account {}
        impl $trait for crate::schema::Blob {}
        impl $trait for crate::schema::Date {}
        impl $trait for crate::schema::Decimal {}
        impl $trait for crate::schema::Duration {}
        impl $trait for crate::schema::Float32 {}
        impl $trait for crate::schema::Float64 {}
        impl $trait for crate::schema::IntBig {}
        impl $trait for crate::schema::NatBig {}
        impl $trait for crate::schema::Principal {}
        impl $trait for crate::schema::Subaccount {}
        impl $trait for crate::schema::Timestamp {}
        impl $trait for crate::schema::Ulid {}
        impl $trait for crate::schema::Unit {}
    };
}
