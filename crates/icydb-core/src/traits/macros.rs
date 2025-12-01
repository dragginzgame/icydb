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
        impl $trait for u8 {}
        impl $trait for u16 {}
        impl $trait for u32 {}
        impl $trait for u64 {}
        impl $trait for f32 {}
        impl $trait for f64 {}
        impl $trait for bool {}
        impl $trait for String {}
    };
}
