#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use icydb::db::query::{
        builder::{QueryBuilder, eq},
        plan::AccessPath,
    };
    use icydb::schema::node::Schema;

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
        let spec = QueryBuilder::<QueryModelEntity>::new()
            .filter(eq("id", Ulid::default()))
            .build();

        let schema = Schema::new();
        let plan = spec
            .plan::<QueryModelEntity>(&schema)
            .expect("plan should not require schema init");

        assert!(matches!(plan.access, AccessPath::ByKey(_)));
    }
}
