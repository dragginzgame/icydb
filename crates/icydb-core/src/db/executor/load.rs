use crate::{
    Error, Key,
    db::{
        Db,
        executor::{FilterEvaluator, plan_for},
        primitives::{FilterDsl, FilterExpr, FilterExt, IntoFilterExpr, Order, SortExpr},
        query::{LoadQuery, QueryPlan, QueryValidate},
        response::Response,
    },
    obs::metrics,
    traits::{EntityKind, FieldValue},
};
use canic::log;
use std::{cmp::Ordering, marker::PhantomData};

///
/// LoadExecutor
///

#[derive(Clone, Copy)]
pub struct LoadExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> LoadExecutor<E> {
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            log!(Debug, "{}", s.into());
        }
    }

    ///
    /// SHORTCUT METHODS
    ///

    pub fn one_key(self, value: impl FieldValue) -> Result<Key, Error> {
        self.one(value)?.try_key()
    }

    pub fn one_entity(self, value: impl FieldValue) -> Result<E, Error> {
        self.one(value)?.try_entity()
    }

    ///
    /// BUILDER METHODS
    ///

    pub fn one(&self, value: impl FieldValue) -> Result<Response<E>, Error> {
        let query = LoadQuery::new().one::<E>(value);
        self.execute(query)
    }

    pub fn only(&self) -> Result<Response<E>, Error> {
        let query = LoadQuery::new().one::<E>(());
        self.execute(query)
    }

    pub fn many(
        &self,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Result<Response<E>, Error> {
        let query = LoadQuery::new().many::<E>(values);
        self.execute(query)
    }

    pub fn all(&self) -> Result<Response<E>, Error> {
        let query = LoadQuery::new();
        self.execute(query)
    }

    pub fn filter<F, I>(self, f: F) -> Result<Response<E>, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        let query = LoadQuery::new().filter(f);
        self.execute(query)
    }

    pub fn count_all(self) -> Result<u32, Error> {
        let query = LoadQuery::all();
        self.count(query)
    }

    ///
    /// EXECUTION METHODS
    ///

    // explain
    pub fn explain(self, query: LoadQuery) -> Result<QueryPlan, Error> {
        QueryValidate::<E>::validate(&query)?;

        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    /// Execute a full query and return a collection of entities.
    pub fn execute(&self, query: LoadQuery) -> Result<Response<E>, Error> {
        let mut span = metrics::Span::<E>::new(metrics::ExecKind::Load);
        QueryValidate::<E>::validate(&query)?;

        self.debug_log(format!("🧭 Executing query: {:?} on {}", query, E::PATH));

        let ctx = self.db.context::<E>();
        let plan = plan_for::<E>(query.filter.as_ref());

        self.debug_log(format!("📄 Query plan: {plan:?}"));

        // Fast path: pre-pagination
        let pre_paginated = query.filter.is_none() && query.sort.is_none() && query.limit.is_some();
        let data_rows = if pre_paginated {
            let lim = query.limit.as_ref().unwrap();
            ctx.rows_from_plan_with_pagination(plan, lim.offset, lim.limit)?
        } else {
            ctx.rows_from_plan(plan)?
        };

        self.debug_log(format!(
            "📦 Loaded {} data rows before deserialization",
            data_rows.len()
        ));

        // Deserialize
        let mut rows: Vec<(Key, E)> = ctx.deserialize_rows(data_rows)?;
        self.debug_log(format!(
            "🧩 Deserialized {} entities before filtering",
            rows.len()
        ));

        // Filtering
        if let Some(f) = &query.filter {
            let simplified = f.clone().simplify();
            Self::apply_filter(&mut rows, &simplified);

            self.debug_log(format!(
                "🔎 Applied filter -> {} entities remaining",
                rows.len()
            ));
        }

        // Sorting
        if let Some(sort) = &query.sort
            && rows.len() > 1
        {
            Self::apply_sort(&mut rows, sort);
            self.debug_log("↕️ Applied sort expression");
        }

        // Pagination
        if let Some(lim) = &query.limit
            && !pre_paginated
        {
            apply_pagination(&mut rows, lim.offset, lim.limit);
            self.debug_log(format!(
                "📏 Applied pagination (offset={}, limit={:?}) -> {} entities",
                lim.offset,
                lim.limit,
                rows.len()
            ));
        }

        crate::db::executor::set_rows_from_len(&mut span, rows.len());
        self.debug_log(format!("✅ Query complete -> {} final rows", rows.len()));

        Ok(Response(rows))
    }

    /// currently just doing the same as execute()
    /// keeping it separate in case we can optimise count queries in the future
    #[allow(clippy::cast_possible_truncation)]
    pub fn count(self, query: LoadQuery) -> Result<u32, Error> {
        let count = self.execute(query)?.count();

        Ok(count)
    }

    // apply_filter
    fn apply_filter(rows: &mut Vec<(Key, E)>, filter: &FilterExpr) {
        rows.retain(|(_, e)| FilterEvaluator::new(e).eval(filter));
    }

    // apply_sort
    fn apply_sort(rows: &mut [(Key, E)], sort_expr: &SortExpr) {
        rows.sort_by(|(_, ea), (_, eb)| {
            for (field, direction) in sort_expr.iter() {
                let va = ea.get_value(field);
                let vb = eb.get_value(field);

                // Define how to handle missing values (None)
                let ordering = match (va, vb) {
                    (None, None) => continue,             // both missing → move to next field
                    (None, Some(_)) => Ordering::Less,    // None sorts before Some(_)
                    (Some(_), None) => Ordering::Greater, // Some(_) sorts after None
                    (Some(va), Some(vb)) => match va.partial_cmp(&vb) {
                        Some(ord) => ord,
                        None => continue, // incomparable values → move to next field
                    },
                };

                // Apply direction (Asc/Desc)
                let ordering = match direction {
                    Order::Asc => ordering,
                    Order::Desc => ordering.reverse(),
                };

                if ordering != Ordering::Equal {
                    return ordering;
                }
            }

            // all fields equal
            Ordering::Equal
        });
    }
}

/// Apply offset/limit pagination to an in-memory vector, in-place.
pub fn apply_pagination<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total = rows.len();
    let start = usize::min(offset as usize, total);
    let end = limit.map_or(total, |l| usize::min(start + l as usize, total));

    if start >= end {
        rows.clear();
    } else {
        rows.drain(..start);
        rows.truncate(end - start);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{LoadExecutor, apply_pagination};
    use crate::{
        IndexSpec, Key, Value,
        db::primitives::{Order, SortExpr},
        traits::{
            CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
            ValidateAuto, ValidateCustom, View, Visitable,
        },
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct SortableEntity {
        id: u64,
        primary: i32,
        secondary: i32,
        optional_blob: Option<Vec<u8>>,
    }

    impl SortableEntity {
        fn new(id: u64, primary: i32, secondary: i32, optional_blob: Option<Vec<u8>>) -> Self {
            Self {
                id,
                primary,
                secondary,
                optional_blob,
            }
        }
    }

    struct SortableCanister;
    struct SortableStore;

    impl Path for SortableCanister {
        const PATH: &'static str = "test::canister";
    }

    impl CanisterKind for SortableCanister {}

    impl Path for SortableStore {
        const PATH: &'static str = "test::store";
    }

    impl StoreKind for SortableStore {
        type Canister = SortableCanister;
    }

    impl Path for SortableEntity {
        const PATH: &'static str = "test::sortable";
    }

    impl View for SortableEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for SortableEntity {}
    impl SanitizeCustom for SortableEntity {}
    impl ValidateAuto for SortableEntity {}
    impl ValidateCustom for SortableEntity {}
    impl Visitable for SortableEntity {}

    impl FieldValues for SortableEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Uint(self.id)),
                "primary" => Some(Value::Int(i64::from(self.primary))),
                "secondary" => Some(Value::Int(i64::from(self.secondary))),
                "optional_blob" => self.optional_blob.clone().map(Value::Blob),
                _ => None,
            }
        }
    }

    impl EntityKind for SortableEntity {
        type PrimaryKey = u64;
        type Store = SortableStore;
        type Canister = SortableCanister;

        const ENTITY_ID: u64 = 99;
        const PRIMARY_KEY: &'static str = "id";
        const FIELDS: &'static [&'static str] = &["id", "primary", "secondary", "optional_blob"];
        const INDEXES: &'static [&'static IndexSpec] = &[];

        fn key(&self) -> Key {
            self.id.into()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.id
        }
    }

    #[test]
    fn pagination_empty_vec() {
        let mut v: Vec<i32> = vec![];
        apply_pagination(&mut v, 0, Some(10));
        assert!(v.is_empty());
    }

    #[test]
    fn pagination_offset_beyond_len_clears() {
        let mut v = vec![1, 2, 3];
        apply_pagination(&mut v, 10, Some(5));
        assert!(v.is_empty());
    }

    #[test]
    fn pagination_no_limit_from_offset() {
        let mut v = vec![1, 2, 3, 4, 5];
        apply_pagination(&mut v, 2, None);
        assert_eq!(v, vec![3, 4, 5]);
    }

    #[test]
    fn pagination_exact_window() {
        let mut v = vec![10, 20, 30, 40, 50];
        // offset 1, limit 3 -> elements [20,30,40]
        apply_pagination(&mut v, 1, Some(3));
        assert_eq!(v, vec![20, 30, 40]);
    }

    #[test]
    fn pagination_limit_exceeds_tail() {
        let mut v = vec![10, 20, 30];
        // offset 1, limit large -> [20,30]
        apply_pagination(&mut v, 1, Some(999));
        assert_eq!(v, vec![20, 30]);
    }

    #[test]
    fn apply_sort_orders_descending() {
        let mut rows = vec![
            (Key::from(1_u64), SortableEntity::new(1, 10, 1, None)),
            (Key::from(2_u64), SortableEntity::new(2, 30, 2, None)),
            (Key::from(3_u64), SortableEntity::new(3, 20, 3, None)),
        ];
        let sort_expr = SortExpr::from(vec![("primary".to_string(), Order::Desc)]);

        LoadExecutor::<SortableEntity>::apply_sort(rows.as_mut_slice(), &sort_expr);

        let primary: Vec<i32> = rows.iter().map(|(_, e)| e.primary).collect();
        assert_eq!(primary, vec![30, 20, 10]);
    }

    #[test]
    fn apply_sort_uses_secondary_field_for_ties() {
        let mut rows = vec![
            (Key::from(1_u64), SortableEntity::new(1, 1, 5, None)),
            (Key::from(2_u64), SortableEntity::new(2, 1, 8, None)),
            (Key::from(3_u64), SortableEntity::new(3, 2, 3, None)),
        ];
        let sort_expr = SortExpr::from(vec![
            ("primary".to_string(), Order::Asc),
            ("secondary".to_string(), Order::Desc),
        ]);

        LoadExecutor::<SortableEntity>::apply_sort(rows.as_mut_slice(), &sort_expr);

        let ids: Vec<u64> = rows.iter().map(|(_, e)| e.id).collect();
        assert_eq!(ids, vec![2, 1, 3]);
    }

    #[test]
    fn apply_sort_places_none_before_some_and_falls_back() {
        let mut rows = vec![
            (
                Key::from(3_u64),
                SortableEntity::new(3, 0, 0, Some(vec![3, 4])),
            ),
            (Key::from(1_u64), SortableEntity::new(1, 0, 0, None)),
            (
                Key::from(2_u64),
                SortableEntity::new(2, 0, 0, Some(vec![2])),
            ),
        ];
        let sort_expr = SortExpr::from(vec![
            ("optional_blob".to_string(), Order::Asc),
            ("id".to_string(), Order::Asc),
        ]);

        LoadExecutor::<SortableEntity>::apply_sort(rows.as_mut_slice(), &sort_expr);

        let ids: Vec<u64> = rows.iter().map(|(_, e)| e.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}
