//! Renamed-dependency typed-adapter compile fixture.

mod schema;

#[cfg(test)]
mod tests {
    use model_api::visitor::Visitable;
    use runtime_api::db::{TypedRowAdapter, TypedWriteAdapter, WriteCell};

    use super::schema::{
        AdapterChoice, AdapterList, AdapterMap, AdapterRecord, AdapterSet, AdapterTuple,
        RecursiveRecord, TypedAdapterEntity, TypedAdapterEntityInsert, TypedAdapterEntityPatch,
        TypedAdapterEntityReplace, X, XEntity,
    };

    fn assert_row_adapter<T: TypedRowAdapter>() {}

    fn assert_write_adapter<T: TypedWriteAdapter>() {}

    fn assert_model_behavior<T: Visitable>() {}

    fn assert_named_adapter<T>()
    where
        T: runtime_api::__macro::TypedInputValue
            + runtime_api::__macro::TypedNamedType
            + runtime_api::__macro::TypedOutputValue,
    {
    }

    #[test]
    fn renamed_dependencies_compile_typed_adapters() {
        let insert_without_database_owned_id = TypedAdapterEntityInsert {
            name: WriteCell::Omitted,
            nickname: WriteCell::Omitted,
            profile: WriteCell::Omitted,
            list: WriteCell::Omitted,
            set: WriteCell::Omitted,
            map: WriteCell::Omitted,
            tuple: WriteCell::Omitted,
            recursive: WriteCell::Omitted,
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
        assert_named_adapter::<X>();
        assert_named_adapter::<XEntity>();
        assert_named_adapter::<AdapterChoice>();
        assert_named_adapter::<AdapterRecord>();
        assert_named_adapter::<AdapterList>();
        assert_named_adapter::<AdapterSet>();
        assert_named_adapter::<AdapterMap>();
        assert_named_adapter::<AdapterTuple>();
        assert_named_adapter::<RecursiveRecord>();
        assert_model_behavior::<runtime_api::types::Ulid>();
    }
}
