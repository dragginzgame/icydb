use crate::db::primitives::filter::{FilterDsl, FilterExpr, FilterKind, IntoScopedFilterExpr};
use candid::CandidType;
use serde::{Deserialize, Serialize};

//
// TextFilterKind
//

pub struct TextFilterKind;

impl FilterKind for TextFilterKind {
    type Payload = TextFilter;
}

//
// TextClause — no mode; mode is encoded in the field (cs/ci)
//

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct TextClause {
    pub values: Vec<String>,
}

impl TextClause {
    pub fn push_value(&mut self, v: impl Into<String>) {
        self.values.push(v.into());
    }
}

//
// TextFilter operators (fully split CS/CI)
//

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct TextFilter {
    pub equal_cs: Option<TextClause>,
    pub equal_ci: Option<TextClause>,

    pub not_equal_cs: Option<TextClause>,
    pub not_equal_ci: Option<TextClause>,

    pub contains_cs: Option<TextClause>,
    pub contains_ci: Option<TextClause>,

    pub starts_with_cs: Option<TextClause>,
    pub starts_with_ci: Option<TextClause>,

    pub ends_with_cs: Option<TextClause>,
    pub ends_with_ci: Option<TextClause>,

    /// Some(true)  → must be empty
    /// Some(false) → must be non-empty
    /// None        → no constraint
    pub is_empty: Option<bool>,
}

impl TextFilter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn is_empty(mut self, yes: bool) -> Self {
        self.is_empty = Some(yes);
        self
    }

    //
    // Equality
    //

    #[must_use]
    pub fn equal(mut self, v: impl Into<String>) -> Self {
        push(&mut self.equal_cs, v);
        self
    }

    #[must_use]
    pub fn equal_ci(mut self, v: impl Into<String>) -> Self {
        push(&mut self.equal_ci, v);
        self
    }

    //
    // Not Equal
    //

    #[must_use]
    pub fn not_equal(mut self, v: impl Into<String>) -> Self {
        push(&mut self.not_equal_cs, v);
        self
    }

    #[must_use]
    pub fn not_equal_ci(mut self, v: impl Into<String>) -> Self {
        push(&mut self.not_equal_ci, v);
        self
    }

    //
    // Contains
    //

    #[must_use]
    pub fn contains(mut self, v: impl Into<String>) -> Self {
        push(&mut self.contains_cs, v);
        self
    }

    #[must_use]
    pub fn contains_ci(mut self, v: impl Into<String>) -> Self {
        push(&mut self.contains_ci, v);
        self
    }

    //
    // Starts With
    //

    #[must_use]
    pub fn starts_with(mut self, v: impl Into<String>) -> Self {
        push(&mut self.starts_with_cs, v);
        self
    }

    #[must_use]
    pub fn starts_with_ci(mut self, v: impl Into<String>) -> Self {
        push(&mut self.starts_with_ci, v);
        self
    }

    //
    // Ends With
    //

    #[must_use]
    pub fn ends_with(mut self, v: impl Into<String>) -> Self {
        push(&mut self.ends_with_cs, v);
        self
    }

    #[must_use]
    pub fn ends_with_ci(mut self, v: impl Into<String>) -> Self {
        push(&mut self.ends_with_ci, v);
        self
    }
}

//
// Shared helper for push() ergonomics
//

fn push(slot: &mut Option<TextClause>, v: impl Into<String>) {
    slot.get_or_insert_with(TextClause::default).push_value(v);
}

impl IntoScopedFilterExpr for TextFilter {
    fn into_scoped(self, field: &str) -> FilterExpr {
        let dsl = FilterDsl;
        let mut parts = Vec::new();

        // Equal
        if let Some(c) = self.equal_cs {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.eq(f, v)));
        }
        if let Some(c) = self.equal_ci {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.eq_ci(f, v)));
        }

        // NotEqual
        if let Some(c) = self.not_equal_cs {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.ne(f, v)));
        }
        if let Some(c) = self.not_equal_ci {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.ne_ci(f, v)));
        }

        // Contains
        if let Some(c) = self.contains_cs {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.contains(f, v)));
        }
        if let Some(c) = self.contains_ci {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.contains_ci(f, v)));
        }

        // StartsWith
        if let Some(c) = self.starts_with_cs {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.starts_with(f, v)));
        }
        if let Some(c) = self.starts_with_ci {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| {
                dsl.starts_with_ci(f, v)
            }));
        }

        // EndsWith
        if let Some(c) = self.ends_with_cs {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.ends_with(f, v)));
        }
        if let Some(c) = self.ends_with_ci {
            parts.push(or_clause(dsl, field, c, |dsl, f, v| dsl.ends_with_ci(f, v)));
        }

        // Empty
        if let Some(flag) = self.is_empty {
            parts.push(if flag {
                dsl.is_empty(field)
            } else {
                dsl.is_not_empty(field)
            });
        }

        FilterDsl::all(parts)
    }
}

//
// OR helper for each clause: ANY(values)
//

fn or_clause(
    dsl: FilterDsl,
    field: &str,
    clause: TextClause,
    mk: impl Fn(FilterDsl, &str, String) -> FilterExpr,
) -> FilterExpr {
    let iter = clause.values.into_iter().map(|v| mk(dsl, field, v));
    FilterDsl::any(iter)
}
