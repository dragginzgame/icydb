//! Renamed-dependency typed-adapter compile fixture.

mod schema;

#[cfg(test)]
mod tests {
    use model_api::visitor::Visitable;
    use runtime_api::db::{TypedRowAdapter, TypedWriteAdapter, WriteCell};

    use super::schema::{
        TypedAdapterEntity, TypedAdapterEntityInsert, TypedAdapterEntityPatch,
        TypedAdapterEntityReplace,
    };

    fn assert_row_adapter<T: TypedRowAdapter>() {}

    fn assert_write_adapter<T: TypedWriteAdapter>() {}

    fn assert_model_behavior<T: Visitable>() {}

    #[test]
    fn renamed_dependencies_compile_typed_adapters() {
        let insert_without_database_owned_id = TypedAdapterEntityInsert {
            name: WriteCell::Omitted,
            nickname: WriteCell::Omitted,
        };
        assert!(matches!(
            insert_without_database_owned_id.name,
            WriteCell::Omitted
        ));
        assert!(matches!(
            insert_without_database_owned_id.nickname,
            WriteCell::Omitted
        ));
        assert_row_adapter::<TypedAdapterEntity>();
        assert_write_adapter::<TypedAdapterEntityInsert>();
        assert_write_adapter::<TypedAdapterEntityPatch>();
        assert_write_adapter::<TypedAdapterEntityReplace>();
        assert_model_behavior::<runtime_api::types::Ulid>();
    }
}
