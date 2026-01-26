#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use icydb::db::query::{Query, ReadConsistency, eq, plan::ExplainAccessPath};

    #[entity(
        store = "TestDataStore",
        pk = "id",
        fields(
            field(ident = "id", value(item(prim = "Ulid"))),
            field(ident = "email", value(item(prim = "Text"))),
        )
    )]
    pub struct QueryModelEntity {}

    #[test]
    fn plan_uses_model_without_schema_init() {
        let query = Query::<QueryModelEntity>::new(ReadConsistency::MissingOk)
            .filter(eq("id", Ulid::default()));
        let plan = query.plan().expect("plan should not require schema init");

        assert!(matches!(
            plan.explain().access,
            ExplainAccessPath::ByKey { .. }
        ));
    }
}
