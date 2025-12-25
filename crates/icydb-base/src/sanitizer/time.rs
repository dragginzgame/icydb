use crate::{core::traits::Sanitizer, prelude::*};

///
/// CreatedAt
///

#[sanitizer]
pub struct CreatedAt;

impl Sanitizer<Timestamp> for CreatedAt {
    fn sanitize(&self, value: &mut Timestamp) -> Result<(), SanitizeIssue> {
        if *value == Timestamp::EPOCH {
            *value = Timestamp::now();
        }

        Ok(())
    }
}

///
/// UpdatedAt
///

#[sanitizer]
pub struct UpdatedAt;

impl Sanitizer<Timestamp> for UpdatedAt {
    fn sanitize(&self, value: &mut Timestamp) -> Result<(), SanitizeIssue> {
        *value = Timestamp::now();

        Ok(())
    }
}
