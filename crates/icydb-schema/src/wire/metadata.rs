//! Native metadata for custom scalar Serde implementations owned by this crate.
//! These describe non-human-readable encoding; browser domain checks remain explicit.

use ic_wire::typescript::{GenerationError, SchemaContext, Wire};
use serde_json::{Value, json};

use crate::{
    Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256, Ulid,
};

// Each declaration follows the scalar's serializer, not its private Rust fields.
macro_rules! scalar_metadata {
    ($ty:ty, $shape:expr) => {
        impl Wire for $ty {
            fn native_schema(_: &mut SchemaContext) -> Result<Value, GenerationError> {
                Ok($shape)
            }
        }
    };
}

scalar_metadata!(Principal, json!({"k":"bytes","max":29}));
scalar_metadata!(Subaccount, json!({"k":"bytes","min":32,"max":32}));
scalar_metadata!(Ulid, json!({"k":"bytes","min":16,"max":16}));
scalar_metadata!(U256, json!({"k":"bytes","min":32,"max":32}));
scalar_metadata!(
    Date,
    json!({"k":"i","bits":32,"min":Date::MIN.as_days_since_epoch(),"max":Date::MAX.as_days_since_epoch()})
);
scalar_metadata!(Decimal, json!({"k":"text","check":"decimal"}));
scalar_metadata!(Duration, json!({"k":"u","bits":64}));
scalar_metadata!(Timestamp, json!({"k":"i","bits":64}));
scalar_metadata!(Float32, json!({"k":"float","bits":32}));
scalar_metadata!(Float64, json!({"k":"float","bits":64}));
scalar_metadata!(crate::EntitySourceKey, json!({"k":"text"}));
