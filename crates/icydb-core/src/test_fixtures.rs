use crate::model::{entity::EntityModel, field::EntityFieldModel, index::IndexModel};

///
/// model_with_fields
///
/// Build a minimal `EntityModel` for tests with default identity and no indexes.
/// Leaks field storage to satisfy static lifetime requirements.
///
pub fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel {
    model_with_fields_and_indexes("test_fixtures::Entity", "TestEntity", fields, pk_index, &[])
}

///
/// model_with_fields_and_indexes
///
/// Build a test `EntityModel` with explicit identity and indexes.
/// Leaks field storage to satisfy static lifetime requirements.
///
pub fn model_with_fields_and_indexes(
    path: &'static str,
    entity_name: &'static str,
    fields: Vec<EntityFieldModel>,
    pk_index: usize,
    indexes: &'static [&'static IndexModel],
) -> EntityModel {
    let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
    let primary_key = &fields[pk_index];

    EntityModel {
        path,
        entity_name,
        primary_key,
        fields,
        indexes,
    }
}
