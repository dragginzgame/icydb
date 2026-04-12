use crate::design::prelude::*;
use icydb_core::sanitize::SanitizeWriteMode;

///
/// CreatedAt
///

#[sanitizer]
pub struct CreatedAt;

impl Sanitizer<Timestamp> for CreatedAt {
    fn sanitize(&self, value: &mut Timestamp) -> Result<(), String> {
        if *value == Timestamp::EPOCH {
            *value = Timestamp::now();
        }

        Ok(())
    }

    fn sanitize_with_context(
        &self,
        value: &mut Timestamp,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), String> {
        let Some(write_context) = ctx.sanitize_write_context() else {
            return self.sanitize(value);
        };

        if matches!(
            write_context.mode(),
            SanitizeWriteMode::Insert | SanitizeWriteMode::Replace
        ) && *value == Timestamp::EPOCH
        {
            *value = write_context.now();
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
    fn sanitize(&self, value: &mut Timestamp) -> Result<(), String> {
        *value = Timestamp::now();

        Ok(())
    }

    fn sanitize_with_context(
        &self,
        value: &mut Timestamp,
        ctx: &mut dyn VisitorContext,
    ) -> Result<(), String> {
        let Some(write_context) = ctx.sanitize_write_context() else {
            return self.sanitize(value);
        };

        *value = write_context.now();

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visitor::{Issue, PathSegment, VisitorContext};
    use icydb_core::sanitize::SanitizeWriteContext;

    struct TestCtx {
        write_context: Option<SanitizeWriteContext>,
    }

    impl TestCtx {
        const fn new(write_context: Option<SanitizeWriteContext>) -> Self {
            Self { write_context }
        }
    }

    impl VisitorContext for TestCtx {
        fn add_issue(&mut self, _issue: Issue) {}

        fn add_issue_at(&mut self, _seg: PathSegment, _issue: Issue) {}

        fn sanitize_write_context(&self) -> Option<SanitizeWriteContext> {
            self.write_context
        }
    }

    #[test]
    fn created_at_insert_and_replace_promote_epoch_to_write_now_matrix() {
        let now = Timestamp::from_nanos(77);

        for (mode, context) in [
            (SanitizeWriteMode::Insert, "insert mode"),
            (SanitizeWriteMode::Replace, "replace mode"),
        ] {
            let mut value = Timestamp::EPOCH;
            let mut ctx = TestCtx::new(Some(SanitizeWriteContext::new(mode, now)));

            CreatedAt
                .sanitize_with_context(&mut value, &mut ctx)
                .unwrap_or_else(|err| panic!("{context} should sanitize successfully: {err}"));

            assert_eq!(value, now, "{context} should promote EPOCH to write now");
        }
    }

    #[test]
    fn created_at_update_preserves_existing_value_matrix() {
        let preserved = Timestamp::from_nanos(33);

        for (initial, context) in [
            (Timestamp::EPOCH, "update should preserve explicit epoch"),
            (preserved, "update should preserve existing created_at"),
        ] {
            let mut value = initial;
            let mut ctx = TestCtx::new(Some(SanitizeWriteContext::new(
                SanitizeWriteMode::Update,
                Timestamp::from_nanos(88),
            )));

            CreatedAt
                .sanitize_with_context(&mut value, &mut ctx)
                .unwrap_or_else(|err| panic!("{context} should sanitize successfully: {err}"));

            assert_eq!(
                value, initial,
                "{context} should leave created_at untouched"
            );
        }
    }

    #[test]
    fn updated_at_write_context_always_uses_write_now_matrix() {
        let now = Timestamp::from_nanos(91);

        for (mode, initial, context) in [
            (
                SanitizeWriteMode::Insert,
                Timestamp::EPOCH,
                "insert mode should stamp updated_at",
            ),
            (
                SanitizeWriteMode::Replace,
                Timestamp::from_nanos(12),
                "replace mode should restamp updated_at",
            ),
            (
                SanitizeWriteMode::Update,
                Timestamp::from_nanos(44),
                "update mode should refresh updated_at",
            ),
        ] {
            let mut value = initial;
            let mut ctx = TestCtx::new(Some(SanitizeWriteContext::new(mode, now)));

            UpdatedAt
                .sanitize_with_context(&mut value, &mut ctx)
                .unwrap_or_else(|err| panic!("{context} should sanitize successfully: {err}"));

            assert_eq!(value, now, "{context} should always use write now");
        }
    }
}
