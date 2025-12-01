use crate::db::primitives::sort::{Order, SortExpr};

///
/// SortSlot
///

pub trait SortSlot {
    fn sort_slot(&mut self) -> &mut Option<SortExpr>;
}

pub trait IntoSortKey {
    fn into_sort_key(self) -> (String, Order);
}

///
/// SortKey Wrappers
///

pub struct Asc<T: Into<String>>(pub T);
pub struct Desc<T: Into<String>>(pub T);

impl<T: Into<String>> IntoSortKey for Asc<T> {
    fn into_sort_key(self) -> (String, Order) {
        (self.0.into(), Order::Asc)
    }
}

impl<T: Into<String>> IntoSortKey for Desc<T> {
    fn into_sort_key(self) -> (String, Order) {
        (self.0.into(), Order::Desc)
    }
}

///
/// SortExprBuilder
///

#[derive(Default)]
pub struct SortExprBuilder {
    keys: Vec<(String, Order)>,
}

impl SortExprBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn asc(mut self, field: impl Into<String>) -> Self {
        self.keys.push((field.into(), Order::Asc));
        self
    }

    #[must_use]
    pub fn desc(mut self, field: impl Into<String>) -> Self {
        self.keys.push((field.into(), Order::Desc));
        self
    }
}

impl From<SortExprBuilder> for SortExpr {
    fn from(b: SortExprBuilder) -> Self {
        b.keys.into()
    }
}

///
/// SortExt
///

impl<T: SortSlot> SortExt for T {}

pub trait SortExt: SortSlot + Sized {
    /// Closure-based DSL (matches `.filter(|f| ...)`)
    #[must_use]
    fn sort<F, R>(mut self, f: F) -> Self
    where
        F: FnOnce(SortExprBuilder) -> R,
        R: Into<SortExpr>,
    {
        let slot = self.sort_slot();
        let expr: SortExpr = f(SortExprBuilder::new()).into();

        if let Some(existing) = slot.as_mut() {
            existing.extend(expr.iter().cloned());
        } else {
            *slot = Some(expr);
        }

        self
    }

    #[must_use]
    fn sort_opt(mut self, expr: Option<SortExpr>) -> Self {
        if let Some(e) = expr {
            self = self.sort(|_| e);
        }

        self
    }

    #[must_use]
    fn sort_by(self, field: &str, order: Order) -> Self {
        match order {
            Order::Asc => self.sort(|s| s.asc(field)),
            Order::Desc => self.sort(|s| s.desc(field)),
        }
    }

    // apply a full SortExpr
    #[must_use]
    fn sort_expr(mut self, expr: SortExpr) -> Self {
        let slot = self.sort_slot();
        if let Some(existing) = slot.as_mut() {
            existing.extend(expr.iter().cloned());
        } else {
            *slot = Some(expr);
        }

        self
    }

    // optional stricter version that replaces instead of merging
    #[must_use]
    fn set_sort_expr(mut self, expr: SortExpr) -> Self {
        *self.sort_slot() = Some(expr);

        self
    }
}
