use crate::db::primitives::limit::LimitExpr;

///
/// LimitSlot
///

pub trait LimitSlot {
    fn limit_slot(&mut self) -> &mut Option<LimitExpr>;
}

///
/// LimitExt
///

impl<T: LimitSlot> LimitExt for T {}

pub trait LimitExt: LimitSlot + Sized {
    #[must_use]
    fn page(self, limit: Option<u32>, offset: Option<u32>) -> Self {
        self.limit_opt(limit).offset_opt(offset)
    }

    #[must_use]
    fn limit(self, limit: u32) -> Self {
        let mut me = self;

        let slot = me.limit_slot();
        let expr = slot.take().unwrap_or_default().limit(limit);
        *slot = Some(expr);

        me
    }

    #[must_use]
    fn limit_opt(self, opt: Option<u32>) -> Self {
        if let Some(limit) = opt {
            self.limit(limit)
        } else {
            self
        }
    }

    #[must_use]
    fn offset(self, offset: u32) -> Self {
        let mut me = self;

        let slot = me.limit_slot();
        let expr = slot.take().unwrap_or_default().offset(offset);
        *slot = Some(expr);

        me
    }

    #[must_use]
    fn offset_opt(self, opt: Option<u32>) -> Self {
        if let Some(offset) = opt {
            self.offset(offset)
        } else {
            self
        }
    }
}
