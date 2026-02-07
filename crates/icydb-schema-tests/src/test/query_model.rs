use crate::prelude::*;

///
/// QueryModelEntity
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid"))),
        field(ident = "email", value(item(prim = "Text"))),
    )
)]
pub struct QueryModelEntity {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::db::query::{Query, ReadConsistency, plan::ExplainAccessPath};

    #[test]
    fn plan_uses_model_without_schema_init() {
        let query = Query::<QueryModelEntity>::new(ReadConsistency::MissingOk)
            .filter(FieldRef::new("id").eq(Ulid::default()));

        let plan = query.plan().expect("plan should not require schema init");

        assert!(matches!(
            plan.explain().access,
            ExplainAccessPath::ByKey { .. }
        ));
    }
}
