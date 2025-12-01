use crate::{core::traits::Sanitizer, prelude::*};

///
/// CreatedAt
///

#[sanitizer]
pub struct CreatedAt;

impl Sanitizer<Timestamp> for CreatedAt {
    fn sanitize(&self, value: Timestamp) -> Timestamp {
        if value == Timestamp::EPOCH {
            Timestamp::now()
        } else {
            value
        }
    }
}

///
/// UpdatedAt
///

#[sanitizer]
pub struct UpdatedAt;

impl Sanitizer<Timestamp> for UpdatedAt {
    fn sanitize(&self, _: Timestamp) -> Timestamp {
        Timestamp::now()
    }
}
