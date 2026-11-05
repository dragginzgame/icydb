//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedIndexPredicate {
    pub(in crate::db) fn bind_test_sql(sql: &str, fields: &[PersistedFieldSnapshot]) -> Self {
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            crate::db::schema::empty_accepted_enum_catalog_for_tests(),
            crate::db::schema::AcceptedCompositeCatalog::empty(),
            crate::db::schema::AcceptedSchemaRevision::INITIAL,
        );
        let predicate =
            crate::db::predicate::parse_sql_predicate(sql).expect("valid current frontend fixture");
        let bound =
            Self::bind_fields(&predicate, fields, &catalog).expect("fixture binds accepted fields");
        let normalized =
            crate::db::predicate::normalize(bound.to_predicate(fields, &catalog, None).unwrap());
        Self::bind_fields(&normalized, fields, &catalog).expect("normalized fixture binds")
    }

    pub(in crate::db) const fn test_non_null(id: u32) -> Self {
        Self::IsNotNull(FieldId::new(id))
    }
}
