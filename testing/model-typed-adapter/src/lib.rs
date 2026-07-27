//! Renamed-dependency typed-adapter compile fixture.

mod schema;

#[cfg(test)]
mod tests {
    use runtime_api::db::{TypedRowAdapter, TypedWriteAdapter};

    use super::schema::{
        TypedAdapterEntity, TypedAdapterEntityInsert, TypedAdapterEntityPatch,
        TypedAdapterEntityReplace,
    };

    fn assert_row_adapter<T: TypedRowAdapter>() {}

    fn assert_write_adapter<T: TypedWriteAdapter>() {}

    #[test]
    fn renamed_dependencies_compile_typed_adapters() {
        assert_row_adapter::<TypedAdapterEntity>();
        assert_write_adapter::<TypedAdapterEntityInsert>();
        assert_write_adapter::<TypedAdapterEntityPatch>();
        assert_write_adapter::<TypedAdapterEntityReplace>();
    }
}
