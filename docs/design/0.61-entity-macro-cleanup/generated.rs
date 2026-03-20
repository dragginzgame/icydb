const COMPLEX_ENTITY_CONST: ::icydb::schema::node::Entity = ::icydb::schema::node::Entity::new(
    ::icydb::schema::node::Def::new(
        module_path!(),
        "ComplexEntity",
        Some("ADMIN TESTS\nset up to test the admin interface\n\n\nComplexEntity"),
    ),
    <TestStore as ::icydb::traits::Path>::PATH,
    ::icydb::schema::node::PrimaryKey::new("id", ::icydb::schema::node::PrimaryKeySource::Internal),
    None,
    &[],
    ::icydb::schema::node::FieldList::new(&[
        ::icydb::schema::node::Field::new(
            "id",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Ulid,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            Some(::icydb::schema::node::Arg::FuncPath("Ulid :: generate")),
        ),
        ::icydb::schema::node::Field::new(
            "string_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Text,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "principal_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Principal,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "blob_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Blob,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "int_candid",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Int,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "int_8",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Int8,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "int_16",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Int16,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "int_32",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Int32,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "int_64",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Int64,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "nat_candid",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Nat,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "nat_8",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Nat8,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "nat_16",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Nat16,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "nat_64",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Nat64,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "e8s",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(
                        <base::types::finance::E8s as ::icydb::traits::Path>::PATH,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "e18s",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(
                        <base::types::finance::E18s as ::icydb::traits::Path>::PATH,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "float_32",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Float32,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "float_64",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Float64,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "bool_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Bool,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "timestamp",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Timestamp,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "utf8_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(
                        <base::types::bytes::Utf8 as ::icydb::traits::Path>::PATH,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "tuple_test",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<Tuple as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "name_many",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Many,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Text,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "name_opt",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Opt,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Text,
                    ),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "record_a",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<RecordA as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "record_opt",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Opt,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<RecordB as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "record_many",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Many,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<RecordB as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "list",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<List as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "map",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<Map as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "set",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<Set as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_complex",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumA as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_complex_opt",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Opt,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumA as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_complex_many",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Many,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumA as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_simple",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumB as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_simple_many",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Many,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumB as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "variant_simple_opt",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::Opt,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Is(<EnumB as ::icydb::traits::Path>::PATH),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "created_at",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Timestamp,
                    ),
                    None,
                    None,
                    &[],
                    &[::icydb::schema::node::TypeSanitizer::new(
                        <icydb::base::sanitizer::time::CreatedAt as ::icydb::traits::Path>::PATH,
                        ::icydb::schema::node::Args(&[]),
                    )],
                    false,
                ),
            ),
            None,
        ),
        ::icydb::schema::node::Field::new(
            "updated_at",
            ::icydb::schema::node::Value::new(
                ::icydb::schema::types::Cardinality::One,
                ::icydb::schema::node::Item::new(
                    ::icydb::schema::node::ItemTarget::Primitive(
                        ::icydb::schema::types::Primitive::Timestamp,
                    ),
                    None,
                    None,
                    &[],
                    &[::icydb::schema::node::TypeSanitizer::new(
                        <icydb::base::sanitizer::time::UpdatedAt as ::icydb::traits::Path>::PATH,
                        ::icydb::schema::node::Args(&[]),
                    )],
                    false,
                ),
            ),
            None,
        ),
    ]),
    ::icydb::schema::node::Type::new(&[], &[]),
);
#[cfg(not(target_arch = "wasm32"))]
# [:: icydb :: __reexports :: ctor :: ctor (anonymous , crate_path = :: icydb :: __reexports :: ctor)]
fn __ctor() {
    ::icydb::schema::build::schema_write().insert_node(::icydb::schema::node::SchemaNode::Entity(
        COMPLEX_ENTITY_CONST,
    ));
}
#[derive(
    :: icydb :: __reexports :: serde :: Deserialize,
    Debug,
    :: icydb :: __reexports :: serde :: Serialize,
    Eq,
    :: icydb :: __reexports :: icydb_derive :: FieldProjection,
    Clone,
    PartialEq,
)]
#[serde(default)]
#[serde(crate = "::icydb::__reexports::serde")]
pub struct ComplexEntity {
    pub(crate) id: ::icydb::types::Ulid,
    pub(crate) string_test: ::icydb::types::Text,
    pub(crate) principal_test: ::icydb::types::Principal,
    pub(crate) blob_test: ::icydb::types::Blob,
    pub(crate) int_candid: ::icydb::types::Int,
    pub(crate) int_8: ::icydb::types::Int8,
    pub(crate) int_16: ::icydb::types::Int16,
    pub(crate) int_32: ::icydb::types::Int32,
    pub(crate) int_64: ::icydb::types::Int64,
    pub(crate) nat_candid: ::icydb::types::Nat,
    pub(crate) nat_8: ::icydb::types::Nat8,
    pub(crate) nat_16: ::icydb::types::Nat16,
    pub(crate) nat_64: ::icydb::types::Nat64,
    pub(crate) e8s: base::types::finance::E8s,
    pub(crate) e18s: base::types::finance::E18s,
    pub(crate) float_32: ::icydb::types::Float32,
    pub(crate) float_64: ::icydb::types::Float64,
    pub(crate) bool_test: ::icydb::types::Bool,
    pub(crate) timestamp: ::icydb::types::Timestamp,
    pub(crate) utf8_test: base::types::bytes::Utf8,
    pub(crate) tuple_test: Tuple,
    pub(crate) name_many: Vec<::icydb::types::Text>,
    pub(crate) name_opt: Option<::icydb::types::Text>,
    pub(crate) record_a: RecordA,
    pub(crate) record_opt: Option<RecordB>,
    pub(crate) record_many: Vec<RecordB>,
    pub(crate) list: List,
    pub(crate) map: Map,
    pub(crate) set: Set,
    pub(crate) variant_complex: EnumA,
    pub(crate) variant_complex_opt: Option<EnumA>,
    pub(crate) variant_complex_many: Vec<EnumA>,
    pub(crate) variant_simple: EnumB,
    pub(crate) variant_simple_many: Vec<EnumB>,
    pub(crate) variant_simple_opt: Option<EnumB>,
    pub(crate) created_at: ::icydb::types::Timestamp,
    pub(crate) updated_at: ::icydb::types::Timestamp,
}
impl ::icydb::traits::Visitable for ComplexEntity {
    fn drive(&self, visitor: &mut dyn ::icydb::visitor::VisitorCore) {
        use ::icydb::visitor::perform_visit;
        perform_visit(visitor, &self.id, "id");
        perform_visit(visitor, &self.string_test, "string_test");
        perform_visit(visitor, &self.principal_test, "principal_test");
        perform_visit(visitor, &self.blob_test, "blob_test");
        perform_visit(visitor, &self.int_candid, "int_candid");
        perform_visit(visitor, &self.int_8, "int_8");
        perform_visit(visitor, &self.int_16, "int_16");
        perform_visit(visitor, &self.int_32, "int_32");
        perform_visit(visitor, &self.int_64, "int_64");
        perform_visit(visitor, &self.nat_candid, "nat_candid");
        perform_visit(visitor, &self.nat_8, "nat_8");
        perform_visit(visitor, &self.nat_16, "nat_16");
        perform_visit(visitor, &self.nat_64, "nat_64");
        perform_visit(visitor, &self.e8s, "e8s");
        perform_visit(visitor, &self.e18s, "e18s");
        perform_visit(visitor, &self.float_32, "float_32");
        perform_visit(visitor, &self.float_64, "float_64");
        perform_visit(visitor, &self.bool_test, "bool_test");
        perform_visit(visitor, &self.timestamp, "timestamp");
        perform_visit(visitor, &self.utf8_test, "utf8_test");
        perform_visit(visitor, &self.tuple_test, "tuple_test");
        perform_visit(visitor, &self.name_many, "name_many");
        perform_visit(visitor, &self.name_opt, "name_opt");
        perform_visit(visitor, &self.record_a, "record_a");
        perform_visit(visitor, &self.record_opt, "record_opt");
        perform_visit(visitor, &self.record_many, "record_many");
        perform_visit(visitor, &self.list, "list");
        perform_visit(visitor, &self.map, "map");
        perform_visit(visitor, &self.set, "set");
        perform_visit(visitor, &self.variant_complex, "variant_complex");
        perform_visit(visitor, &self.variant_complex_opt, "variant_complex_opt");
        perform_visit(visitor, &self.variant_complex_many, "variant_complex_many");
        perform_visit(visitor, &self.variant_simple, "variant_simple");
        perform_visit(visitor, &self.variant_simple_many, "variant_simple_many");
        perform_visit(visitor, &self.variant_simple_opt, "variant_simple_opt");
        perform_visit(visitor, &self.created_at, "created_at");
        perform_visit(visitor, &self.updated_at, "updated_at");
    }
    fn drive_mut(&mut self, visitor: &mut dyn ::icydb::visitor::VisitorMutCore) {
        use ::icydb::visitor::perform_visit_mut;
        perform_visit_mut(visitor, &mut self.id, "id");
        perform_visit_mut(visitor, &mut self.string_test, "string_test");
        perform_visit_mut(visitor, &mut self.principal_test, "principal_test");
        perform_visit_mut(visitor, &mut self.blob_test, "blob_test");
        perform_visit_mut(visitor, &mut self.int_candid, "int_candid");
        perform_visit_mut(visitor, &mut self.int_8, "int_8");
        perform_visit_mut(visitor, &mut self.int_16, "int_16");
        perform_visit_mut(visitor, &mut self.int_32, "int_32");
        perform_visit_mut(visitor, &mut self.int_64, "int_64");
        perform_visit_mut(visitor, &mut self.nat_candid, "nat_candid");
        perform_visit_mut(visitor, &mut self.nat_8, "nat_8");
        perform_visit_mut(visitor, &mut self.nat_16, "nat_16");
        perform_visit_mut(visitor, &mut self.nat_64, "nat_64");
        perform_visit_mut(visitor, &mut self.e8s, "e8s");
        perform_visit_mut(visitor, &mut self.e18s, "e18s");
        perform_visit_mut(visitor, &mut self.float_32, "float_32");
        perform_visit_mut(visitor, &mut self.float_64, "float_64");
        perform_visit_mut(visitor, &mut self.bool_test, "bool_test");
        perform_visit_mut(visitor, &mut self.timestamp, "timestamp");
        perform_visit_mut(visitor, &mut self.utf8_test, "utf8_test");
        perform_visit_mut(visitor, &mut self.tuple_test, "tuple_test");
        perform_visit_mut(visitor, &mut self.name_many, "name_many");
        perform_visit_mut(visitor, &mut self.name_opt, "name_opt");
        perform_visit_mut(visitor, &mut self.record_a, "record_a");
        perform_visit_mut(visitor, &mut self.record_opt, "record_opt");
        perform_visit_mut(visitor, &mut self.record_many, "record_many");
        perform_visit_mut(visitor, &mut self.list, "list");
        perform_visit_mut(visitor, &mut self.map, "map");
        perform_visit_mut(visitor, &mut self.set, "set");
        perform_visit_mut(visitor, &mut self.variant_complex, "variant_complex");
        perform_visit_mut(
            visitor,
            &mut self.variant_complex_opt,
            "variant_complex_opt",
        );
        perform_visit_mut(
            visitor,
            &mut self.variant_complex_many,
            "variant_complex_many",
        );
        perform_visit_mut(visitor, &mut self.variant_simple, "variant_simple");
        perform_visit_mut(
            visitor,
            &mut self.variant_simple_many,
            "variant_simple_many",
        );
        perform_visit_mut(visitor, &mut self.variant_simple_opt, "variant_simple_opt");
        perform_visit_mut(visitor, &mut self.created_at, "created_at");
        perform_visit_mut(visitor, &mut self.updated_at, "updated_at");
    }
}
impl ::icydb::traits::EntityKey for ComplexEntity {
    type Key = ::icydb::types::Ulid;
}
impl ::icydb::traits::EntityIdentity for ComplexEntity {
    const ENTITY_NAME: &'static str = stringify!(ComplexEntity);
    const PRIMARY_KEY: &'static str = stringify!(id);
}
impl ::icydb::traits::EntitySchema for ComplexEntity {
    const FIELDS: &'static [&'static str] = &[
        Self::ID.as_str(),
        Self::STRING_TEST.as_str(),
        Self::PRINCIPAL_TEST.as_str(),
        Self::BLOB_TEST.as_str(),
        Self::INT_CANDID.as_str(),
        Self::INT_8.as_str(),
        Self::INT_16.as_str(),
        Self::INT_32.as_str(),
        Self::INT_64.as_str(),
        Self::NAT_CANDID.as_str(),
        Self::NAT_8.as_str(),
        Self::NAT_16.as_str(),
        Self::NAT_64.as_str(),
        Self::E8S.as_str(),
        Self::E18S.as_str(),
        Self::FLOAT_32.as_str(),
        Self::FLOAT_64.as_str(),
        Self::BOOL_TEST.as_str(),
        Self::TIMESTAMP.as_str(),
        Self::UTF8_TEST.as_str(),
        Self::TUPLE_TEST.as_str(),
        Self::NAME_MANY.as_str(),
        Self::NAME_OPT.as_str(),
        Self::RECORD_A.as_str(),
        Self::RECORD_OPT.as_str(),
        Self::RECORD_MANY.as_str(),
        Self::LIST.as_str(),
        Self::MAP.as_str(),
        Self::SET.as_str(),
        Self::VARIANT_COMPLEX.as_str(),
        Self::VARIANT_COMPLEX_OPT.as_str(),
        Self::VARIANT_COMPLEX_MANY.as_str(),
        Self::VARIANT_SIMPLE.as_str(),
        Self::VARIANT_SIMPLE_MANY.as_str(),
        Self::VARIANT_SIMPLE_OPT.as_str(),
        Self::CREATED_AT.as_str(),
        Self::UPDATED_AT.as_str(),
    ];
    const INDEXES: &'static [&'static ::icydb::model::index::IndexModel] = &[];
    const MODEL: &'static ::icydb::model::entity::EntityModel = &Self::__ENTITY_MODEL;
}
impl ::icydb::traits::EntityPlacement for ComplexEntity {
    type Store = TestStore;
    type Canister = <Self::Store as ::icydb::traits::StoreKind>::Canister;
}
impl ComplexEntity {
    #[doc(hidden)]
    pub const __ENTITY_TAG_CONST: ::icydb::types::EntityTag = {
        const RAW_ENTITY_TAG: u64 = 13596795643545470544u64;
        ::icydb::types::EntityTag::new(RAW_ENTITY_TAG)
    };
}
impl ::icydb::traits::EntityKind for ComplexEntity {
    const ENTITY_TAG: ::icydb::types::EntityTag = Self::__ENTITY_TAG_CONST;
}
#[cfg(test)]
mod __entity_model_test_ComplexEntity {
    use super::*;
    #[test]
    fn model_consistency() {
        let model = <ComplexEntity as ::icydb::traits::EntitySchema>::MODEL;
        let names = <ComplexEntity as ::icydb::traits::EntitySchema>::FIELDS;
        assert_eq!(model.fields().len(), names.len());
        for (field, name) in model.fields().iter().zip(names.iter()) {
            assert_eq!(field.name(), *name);
        }
        assert!(
            model
                .fields()
                .iter()
                .any(|field| ::core::ptr::eq(field, model.primary_key()))
        );
    }
}
impl ::icydb::traits::ValidateCustom for ComplexEntity {}
impl ::icydb::traits::Default for ComplexEntity {
    fn default() -> Self {
        Self {
            id: ::icydb::__macro::CoreAsView::from_view(Ulid::generate()),
            string_test: Default::default(),
            principal_test: Default::default(),
            blob_test: Default::default(),
            int_candid: Default::default(),
            int_8: Default::default(),
            int_16: Default::default(),
            int_32: Default::default(),
            int_64: Default::default(),
            nat_candid: Default::default(),
            nat_8: Default::default(),
            nat_16: Default::default(),
            nat_64: Default::default(),
            e8s: Default::default(),
            e18s: Default::default(),
            float_32: Default::default(),
            float_64: Default::default(),
            bool_test: Default::default(),
            timestamp: Default::default(),
            utf8_test: Default::default(),
            tuple_test: Default::default(),
            name_many: Vec::default(),
            name_opt: None,
            record_a: Default::default(),
            record_opt: None,
            record_many: Vec::default(),
            list: Default::default(),
            map: Default::default(),
            set: Default::default(),
            variant_complex: Default::default(),
            variant_complex_opt: None,
            variant_complex_many: Vec::default(),
            variant_simple: Default::default(),
            variant_simple_many: Vec::default(),
            variant_simple_opt: None,
            created_at: Default::default(),
            updated_at: Default::default(),
        }
    }
}
impl ::icydb::traits::EntityValue for ComplexEntity {
    fn id(&self) -> ::icydb::types::Id<Self> {
        ::icydb::types::Id::from_key(self.id)
    }
}
impl ::icydb::traits::SanitizeAuto for ComplexEntity {
    fn sanitize_self(&mut self, ctx: &mut dyn ::icydb::visitor::VisitorContext) {
        if let Err(msg) = icydb::base::sanitizer::time::CreatedAt.sanitize(&mut self.created_at) {
            ctx.issue_at(
                ::icydb::visitor::PathSegment::Field(stringify!(created_at)),
                msg,
            );
        }
        if let Err(msg) = icydb::base::sanitizer::time::UpdatedAt.sanitize(&mut self.updated_at) {
            ctx.issue_at(
                ::icydb::visitor::PathSegment::Field(stringify!(updated_at)),
                msg,
            );
        }
    }
}
impl ::icydb::__macro::CoreUpdateView for ComplexEntity {
    type UpdateViewType = complex_entity_views::ComplexEntityUpdate;
    fn merge(
        &mut self,
        patch: Self::UpdateViewType,
    ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
        let mut next = self.clone();
        if let Some(v) = patch.string_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.string_test, v)
                .map_err(|err| err.with_field(stringify!(string_test)))?;
        }
        if let Some(v) = patch.principal_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.principal_test, v)
                .map_err(|err| err.with_field(stringify!(principal_test)))?;
        }
        if let Some(v) = patch.blob_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.blob_test, v)
                .map_err(|err| err.with_field(stringify!(blob_test)))?;
        }
        if let Some(v) = patch.int_candid {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.int_candid, v)
                .map_err(|err| err.with_field(stringify!(int_candid)))?;
        }
        if let Some(v) = patch.int_8 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.int_8, v)
                .map_err(|err| err.with_field(stringify!(int_8)))?;
        }
        if let Some(v) = patch.int_16 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.int_16, v)
                .map_err(|err| err.with_field(stringify!(int_16)))?;
        }
        if let Some(v) = patch.int_32 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.int_32, v)
                .map_err(|err| err.with_field(stringify!(int_32)))?;
        }
        if let Some(v) = patch.int_64 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.int_64, v)
                .map_err(|err| err.with_field(stringify!(int_64)))?;
        }
        if let Some(v) = patch.nat_candid {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.nat_candid, v)
                .map_err(|err| err.with_field(stringify!(nat_candid)))?;
        }
        if let Some(v) = patch.nat_8 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.nat_8, v)
                .map_err(|err| err.with_field(stringify!(nat_8)))?;
        }
        if let Some(v) = patch.nat_16 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.nat_16, v)
                .map_err(|err| err.with_field(stringify!(nat_16)))?;
        }
        if let Some(v) = patch.nat_64 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.nat_64, v)
                .map_err(|err| err.with_field(stringify!(nat_64)))?;
        }
        if let Some(v) = patch.e8s {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.e8s, v)
                .map_err(|err| err.with_field(stringify!(e8s)))?;
        }
        if let Some(v) = patch.e18s {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.e18s, v)
                .map_err(|err| err.with_field(stringify!(e18s)))?;
        }
        if let Some(v) = patch.float_32 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.float_32, v)
                .map_err(|err| err.with_field(stringify!(float_32)))?;
        }
        if let Some(v) = patch.float_64 {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.float_64, v)
                .map_err(|err| err.with_field(stringify!(float_64)))?;
        }
        if let Some(v) = patch.bool_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.bool_test, v)
                .map_err(|err| err.with_field(stringify!(bool_test)))?;
        }
        if let Some(v) = patch.timestamp {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.timestamp, v)
                .map_err(|err| err.with_field(stringify!(timestamp)))?;
        }
        if let Some(v) = patch.utf8_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.utf8_test, v)
                .map_err(|err| err.with_field(stringify!(utf8_test)))?;
        }
        if let Some(v) = patch.tuple_test {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.tuple_test, v)
                .map_err(|err| err.with_field(stringify!(tuple_test)))?;
        }
        if let Some(v) = patch.name_many {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.name_many, v)
                .map_err(|err| err.with_field(stringify!(name_many)))?;
        }
        if let Some(v) = patch.name_opt {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.name_opt, v)
                .map_err(|err| err.with_field(stringify!(name_opt)))?;
        }
        if let Some(v) = patch.record_a {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.record_a, v)
                .map_err(|err| err.with_field(stringify!(record_a)))?;
        }
        if let Some(v) = patch.record_opt {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.record_opt, v)
                .map_err(|err| err.with_field(stringify!(record_opt)))?;
        }
        if let Some(v) = patch.record_many {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.record_many, v)
                .map_err(|err| err.with_field(stringify!(record_many)))?;
        }
        if let Some(v) = patch.list {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.list, v)
                .map_err(|err| err.with_field(stringify!(list)))?;
        }
        if let Some(v) = patch.map {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.map, v)
                .map_err(|err| err.with_field(stringify!(map)))?;
        }
        if let Some(v) = patch.set {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.set, v)
                .map_err(|err| err.with_field(stringify!(set)))?;
        }
        if let Some(v) = patch.variant_complex {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_complex, v)
                .map_err(|err| err.with_field(stringify!(variant_complex)))?;
        }
        if let Some(v) = patch.variant_complex_opt {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_complex_opt, v)
                .map_err(|err| err.with_field(stringify!(variant_complex_opt)))?;
        }
        if let Some(v) = patch.variant_complex_many {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_complex_many, v)
                .map_err(|err| err.with_field(stringify!(variant_complex_many)))?;
        }
        if let Some(v) = patch.variant_simple {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_simple, v)
                .map_err(|err| err.with_field(stringify!(variant_simple)))?;
        }
        if let Some(v) = patch.variant_simple_many {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_simple_many, v)
                .map_err(|err| err.with_field(stringify!(variant_simple_many)))?;
        }
        if let Some(v) = patch.variant_simple_opt {
            ::icydb::__macro::CoreUpdateView::merge(&mut next.variant_simple_opt, v)
                .map_err(|err| err.with_field(stringify!(variant_simple_opt)))?;
        }
        *self = next;
        Ok(())
    }
}
impl ComplexEntity {
    pub const ID: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("id");
    pub const STRING_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("string_test");
    pub const PRINCIPAL_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("principal_test");
    pub const BLOB_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("blob_test");
    pub const INT_CANDID: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("int_candid");
    pub const INT_8: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("int_8");
    pub const INT_16: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("int_16");
    pub const INT_32: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("int_32");
    pub const INT_64: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("int_64");
    pub const NAT_CANDID: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("nat_candid");
    pub const NAT_8: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("nat_8");
    pub const NAT_16: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("nat_16");
    pub const NAT_64: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("nat_64");
    pub const E8S: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("e8s");
    pub const E18S: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("e18s");
    pub const FLOAT_32: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("float_32");
    pub const FLOAT_64: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("float_64");
    pub const BOOL_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("bool_test");
    pub const TIMESTAMP: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("timestamp");
    pub const UTF8_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("utf8_test");
    pub const TUPLE_TEST: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("tuple_test");
    pub const NAME_MANY: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("name_many");
    pub const NAME_OPT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("name_opt");
    pub const RECORD_A: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("record_a");
    pub const RECORD_OPT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("record_opt");
    pub const RECORD_MANY: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("record_many");
    pub const LIST: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("list");
    pub const MAP: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("map");
    pub const SET: ::icydb::db::query::FieldRef = ::icydb::db::query::FieldRef::new("set");
    pub const VARIANT_COMPLEX: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_complex");
    pub const VARIANT_COMPLEX_OPT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_complex_opt");
    pub const VARIANT_COMPLEX_MANY: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_complex_many");
    pub const VARIANT_SIMPLE: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_simple");
    pub const VARIANT_SIMPLE_MANY: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_simple_many");
    pub const VARIANT_SIMPLE_OPT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("variant_simple_opt");
    pub const CREATED_AT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("created_at");
    pub const UPDATED_AT: ::icydb::db::query::FieldRef =
        ::icydb::db::query::FieldRef::new("updated_at");
    const __MODEL_FIELD_ID: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("id", ::icydb::model::field::FieldKind::Ulid);
    const __MODEL_FIELD_STRING_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "string_test",
            ::icydb::model::field::FieldKind::Text,
        );
    const __MODEL_FIELD_PRINCIPAL_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "principal_test",
            ::icydb::model::field::FieldKind::Principal,
        );
    const __MODEL_FIELD_BLOB_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("blob_test", ::icydb::model::field::FieldKind::Blob);
    const __MODEL_FIELD_INT_CANDID: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "int_candid",
            ::icydb::model::field::FieldKind::IntBig,
        );
    const __MODEL_FIELD_INT_8: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("int_8", ::icydb::model::field::FieldKind::Int);
    const __MODEL_FIELD_INT_16: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("int_16", ::icydb::model::field::FieldKind::Int);
    const __MODEL_FIELD_INT_32: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("int_32", ::icydb::model::field::FieldKind::Int);
    const __MODEL_FIELD_INT_64: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("int_64", ::icydb::model::field::FieldKind::Int);
    const __MODEL_FIELD_NAT_CANDID: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "nat_candid",
            ::icydb::model::field::FieldKind::UintBig,
        );
    const __MODEL_FIELD_NAT_8: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("nat_8", ::icydb::model::field::FieldKind::Uint);
    const __MODEL_FIELD_NAT_16: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("nat_16", ::icydb::model::field::FieldKind::Uint);
    const __MODEL_FIELD_NAT_64: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("nat_64", ::icydb::model::field::FieldKind::Uint);
    const __MODEL_FIELD_E8S: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("e8s", base::types::finance::E8s::KIND);
    const __MODEL_FIELD_E18S: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("e18s", base::types::finance::E18s::KIND);
    const __MODEL_FIELD_FLOAT_32: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "float_32",
            ::icydb::model::field::FieldKind::Float32,
        );
    const __MODEL_FIELD_FLOAT_64: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "float_64",
            ::icydb::model::field::FieldKind::Float64,
        );
    const __MODEL_FIELD_BOOL_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("bool_test", ::icydb::model::field::FieldKind::Bool);
    const __MODEL_FIELD_TIMESTAMP: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "timestamp",
            ::icydb::model::field::FieldKind::Timestamp,
        );
    const __MODEL_FIELD_UTF8_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("utf8_test", base::types::bytes::Utf8::KIND);
    const __MODEL_FIELD_TUPLE_TEST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("tuple_test", Tuple::KIND);
    const __MODEL_FIELD_NAME_MANY: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "name_many",
            ::icydb::model::field::FieldKind::List(&::icydb::model::field::FieldKind::Text),
        );
    const __MODEL_FIELD_NAME_OPT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("name_opt", ::icydb::model::field::FieldKind::Text);
    const __MODEL_FIELD_RECORD_A: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("record_a", RecordA::KIND);
    const __MODEL_FIELD_RECORD_OPT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("record_opt", RecordB::KIND);
    const __MODEL_FIELD_RECORD_MANY: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "record_many",
            ::icydb::model::field::FieldKind::List(&RecordB::KIND),
        );
    const __MODEL_FIELD_LIST: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("list", List::KIND);
    const __MODEL_FIELD_MAP: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("map", Map::KIND);
    const __MODEL_FIELD_SET: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("set", Set::KIND);
    const __MODEL_FIELD_VARIANT_COMPLEX: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("variant_complex", EnumA::KIND);
    const __MODEL_FIELD_VARIANT_COMPLEX_OPT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("variant_complex_opt", EnumA::KIND);
    const __MODEL_FIELD_VARIANT_COMPLEX_MANY: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "variant_complex_many",
            ::icydb::model::field::FieldKind::List(&EnumA::KIND),
        );
    const __MODEL_FIELD_VARIANT_SIMPLE: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("variant_simple", EnumB::KIND);
    const __MODEL_FIELD_VARIANT_SIMPLE_MANY: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "variant_simple_many",
            ::icydb::model::field::FieldKind::List(&EnumB::KIND),
        );
    const __MODEL_FIELD_VARIANT_SIMPLE_OPT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new("variant_simple_opt", EnumB::KIND);
    const __MODEL_FIELD_CREATED_AT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "created_at",
            ::icydb::model::field::FieldKind::Timestamp,
        );
    const __MODEL_FIELD_UPDATED_AT: ::icydb::model::field::FieldModel =
        ::icydb::model::field::FieldModel::new(
            "updated_at",
            ::icydb::model::field::FieldKind::Timestamp,
        );
    const __MODEL_FIELDS: [::icydb::model::field::FieldModel; 37] = [
        Self::__MODEL_FIELD_ID,
        Self::__MODEL_FIELD_STRING_TEST,
        Self::__MODEL_FIELD_PRINCIPAL_TEST,
        Self::__MODEL_FIELD_BLOB_TEST,
        Self::__MODEL_FIELD_INT_CANDID,
        Self::__MODEL_FIELD_INT_8,
        Self::__MODEL_FIELD_INT_16,
        Self::__MODEL_FIELD_INT_32,
        Self::__MODEL_FIELD_INT_64,
        Self::__MODEL_FIELD_NAT_CANDID,
        Self::__MODEL_FIELD_NAT_8,
        Self::__MODEL_FIELD_NAT_16,
        Self::__MODEL_FIELD_NAT_64,
        Self::__MODEL_FIELD_E8S,
        Self::__MODEL_FIELD_E18S,
        Self::__MODEL_FIELD_FLOAT_32,
        Self::__MODEL_FIELD_FLOAT_64,
        Self::__MODEL_FIELD_BOOL_TEST,
        Self::__MODEL_FIELD_TIMESTAMP,
        Self::__MODEL_FIELD_UTF8_TEST,
        Self::__MODEL_FIELD_TUPLE_TEST,
        Self::__MODEL_FIELD_NAME_MANY,
        Self::__MODEL_FIELD_NAME_OPT,
        Self::__MODEL_FIELD_RECORD_A,
        Self::__MODEL_FIELD_RECORD_OPT,
        Self::__MODEL_FIELD_RECORD_MANY,
        Self::__MODEL_FIELD_LIST,
        Self::__MODEL_FIELD_MAP,
        Self::__MODEL_FIELD_SET,
        Self::__MODEL_FIELD_VARIANT_COMPLEX,
        Self::__MODEL_FIELD_VARIANT_COMPLEX_OPT,
        Self::__MODEL_FIELD_VARIANT_COMPLEX_MANY,
        Self::__MODEL_FIELD_VARIANT_SIMPLE,
        Self::__MODEL_FIELD_VARIANT_SIMPLE_MANY,
        Self::__MODEL_FIELD_VARIANT_SIMPLE_OPT,
        Self::__MODEL_FIELD_CREATED_AT,
        Self::__MODEL_FIELD_UPDATED_AT,
    ];
    const __ENTITY_MODEL: ::icydb::model::entity::EntityModel =
        ::icydb::model::entity::EntityModel::new(
            <Self as ::icydb::traits::Path>::PATH,
            <Self as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
            &Self::__MODEL_FIELDS[0],
            &Self::__MODEL_FIELDS,
            <Self as ::icydb::traits::EntitySchema>::INDEXES,
        );
}
impl ::icydb::traits::Path for ComplexEntity {
    const PATH: &'static str = concat!(module_path!(), "::", stringify!(ComplexEntity));
}
impl ::icydb::__macro::CoreAsView for ComplexEntity {
    type ViewType = complex_entity_views::ComplexEntityView;
    fn as_view(&self) -> Self::ViewType {
        complex_entity_views::ComplexEntityView {
            id: ::icydb::__macro::CoreAsView::as_view(&self.id),
            string_test: ::icydb::__macro::CoreAsView::as_view(&self.string_test),
            principal_test: ::icydb::__macro::CoreAsView::as_view(&self.principal_test),
            blob_test: ::icydb::__macro::CoreAsView::as_view(&self.blob_test),
            int_candid: ::icydb::__macro::CoreAsView::as_view(&self.int_candid),
            int_8: ::icydb::__macro::CoreAsView::as_view(&self.int_8),
            int_16: ::icydb::__macro::CoreAsView::as_view(&self.int_16),
            int_32: ::icydb::__macro::CoreAsView::as_view(&self.int_32),
            int_64: ::icydb::__macro::CoreAsView::as_view(&self.int_64),
            nat_candid: ::icydb::__macro::CoreAsView::as_view(&self.nat_candid),
            nat_8: ::icydb::__macro::CoreAsView::as_view(&self.nat_8),
            nat_16: ::icydb::__macro::CoreAsView::as_view(&self.nat_16),
            nat_64: ::icydb::__macro::CoreAsView::as_view(&self.nat_64),
            e8s: ::icydb::__macro::CoreAsView::as_view(&self.e8s),
            e18s: ::icydb::__macro::CoreAsView::as_view(&self.e18s),
            float_32: ::icydb::__macro::CoreAsView::as_view(&self.float_32),
            float_64: ::icydb::__macro::CoreAsView::as_view(&self.float_64),
            bool_test: ::icydb::__macro::CoreAsView::as_view(&self.bool_test),
            timestamp: ::icydb::__macro::CoreAsView::as_view(&self.timestamp),
            utf8_test: ::icydb::__macro::CoreAsView::as_view(&self.utf8_test),
            tuple_test: ::icydb::__macro::CoreAsView::as_view(&self.tuple_test),
            name_many: ::icydb::__macro::CoreAsView::as_view(&self.name_many),
            name_opt: ::icydb::__macro::CoreAsView::as_view(&self.name_opt),
            record_a: ::icydb::__macro::CoreAsView::as_view(&self.record_a),
            record_opt: ::icydb::__macro::CoreAsView::as_view(&self.record_opt),
            record_many: ::icydb::__macro::CoreAsView::as_view(&self.record_many),
            list: ::icydb::__macro::CoreAsView::as_view(&self.list),
            map: ::icydb::__macro::CoreAsView::as_view(&self.map),
            set: ::icydb::__macro::CoreAsView::as_view(&self.set),
            variant_complex: ::icydb::__macro::CoreAsView::as_view(&self.variant_complex),
            variant_complex_opt: ::icydb::__macro::CoreAsView::as_view(&self.variant_complex_opt),
            variant_complex_many: ::icydb::__macro::CoreAsView::as_view(&self.variant_complex_many),
            variant_simple: ::icydb::__macro::CoreAsView::as_view(&self.variant_simple),
            variant_simple_many: ::icydb::__macro::CoreAsView::as_view(&self.variant_simple_many),
            variant_simple_opt: ::icydb::__macro::CoreAsView::as_view(&self.variant_simple_opt),
            created_at: ::icydb::__macro::CoreAsView::as_view(&self.created_at),
            updated_at: ::icydb::__macro::CoreAsView::as_view(&self.updated_at),
        }
    }
    fn from_view(view: Self::ViewType) -> Self {
        Self {
            id: ::icydb::__macro::CoreAsView::from_view(view.id),
            string_test: ::icydb::__macro::CoreAsView::from_view(view.string_test),
            principal_test: ::icydb::__macro::CoreAsView::from_view(view.principal_test),
            blob_test: ::icydb::__macro::CoreAsView::from_view(view.blob_test),
            int_candid: ::icydb::__macro::CoreAsView::from_view(view.int_candid),
            int_8: ::icydb::__macro::CoreAsView::from_view(view.int_8),
            int_16: ::icydb::__macro::CoreAsView::from_view(view.int_16),
            int_32: ::icydb::__macro::CoreAsView::from_view(view.int_32),
            int_64: ::icydb::__macro::CoreAsView::from_view(view.int_64),
            nat_candid: ::icydb::__macro::CoreAsView::from_view(view.nat_candid),
            nat_8: ::icydb::__macro::CoreAsView::from_view(view.nat_8),
            nat_16: ::icydb::__macro::CoreAsView::from_view(view.nat_16),
            nat_64: ::icydb::__macro::CoreAsView::from_view(view.nat_64),
            e8s: ::icydb::__macro::CoreAsView::from_view(view.e8s),
            e18s: ::icydb::__macro::CoreAsView::from_view(view.e18s),
            float_32: ::icydb::__macro::CoreAsView::from_view(view.float_32),
            float_64: ::icydb::__macro::CoreAsView::from_view(view.float_64),
            bool_test: ::icydb::__macro::CoreAsView::from_view(view.bool_test),
            timestamp: ::icydb::__macro::CoreAsView::from_view(view.timestamp),
            utf8_test: ::icydb::__macro::CoreAsView::from_view(view.utf8_test),
            tuple_test: ::icydb::__macro::CoreAsView::from_view(view.tuple_test),
            name_many: ::icydb::__macro::CoreAsView::from_view(view.name_many),
            name_opt: ::icydb::__macro::CoreAsView::from_view(view.name_opt),
            record_a: ::icydb::__macro::CoreAsView::from_view(view.record_a),
            record_opt: ::icydb::__macro::CoreAsView::from_view(view.record_opt),
            record_many: ::icydb::__macro::CoreAsView::from_view(view.record_many),
            list: ::icydb::__macro::CoreAsView::from_view(view.list),
            map: ::icydb::__macro::CoreAsView::from_view(view.map),
            set: ::icydb::__macro::CoreAsView::from_view(view.set),
            variant_complex: ::icydb::__macro::CoreAsView::from_view(view.variant_complex),
            variant_complex_opt: ::icydb::__macro::CoreAsView::from_view(view.variant_complex_opt),
            variant_complex_many: ::icydb::__macro::CoreAsView::from_view(
                view.variant_complex_many,
            ),
            variant_simple: ::icydb::__macro::CoreAsView::from_view(view.variant_simple),
            variant_simple_many: ::icydb::__macro::CoreAsView::from_view(view.variant_simple_many),
            variant_simple_opt: ::icydb::__macro::CoreAsView::from_view(view.variant_simple_opt),
            created_at: ::icydb::__macro::CoreAsView::from_view(view.created_at),
            updated_at: ::icydb::__macro::CoreAsView::from_view(view.updated_at),
        }
    }
}
impl From<ComplexEntity> for complex_entity_views::ComplexEntityView {
    fn from(value: ComplexEntity) -> Self {
        ::icydb::__macro::CoreAsView::as_view(&value)
    }
}
impl From<&ComplexEntity> for complex_entity_views::ComplexEntityView {
    fn from(value: &ComplexEntity) -> Self {
        ::icydb::__macro::CoreAsView::as_view(value)
    }
}
impl From<complex_entity_views::ComplexEntityView> for ComplexEntity {
    fn from(view: complex_entity_views::ComplexEntityView) -> Self {
        ::icydb::__macro::CoreAsView::from_view(view)
    }
}
impl ::icydb::__macro::CoreCreateView for ComplexEntity {
    type CreateViewType = complex_entity_views::ComplexEntityCreate;
    fn from_create_view(view: Self::CreateViewType) -> Self {
        view.into()
    }
}
impl From<complex_entity_views::ComplexEntityCreate> for ComplexEntity {
    fn from(create: complex_entity_views::ComplexEntityCreate) -> Self {
        Self {
            string_test: ::icydb::__macro::CoreAsView::from_view(create.string_test),
            principal_test: ::icydb::__macro::CoreAsView::from_view(create.principal_test),
            blob_test: ::icydb::__macro::CoreAsView::from_view(create.blob_test),
            int_candid: ::icydb::__macro::CoreAsView::from_view(create.int_candid),
            int_8: ::icydb::__macro::CoreAsView::from_view(create.int_8),
            int_16: ::icydb::__macro::CoreAsView::from_view(create.int_16),
            int_32: ::icydb::__macro::CoreAsView::from_view(create.int_32),
            int_64: ::icydb::__macro::CoreAsView::from_view(create.int_64),
            nat_candid: ::icydb::__macro::CoreAsView::from_view(create.nat_candid),
            nat_8: ::icydb::__macro::CoreAsView::from_view(create.nat_8),
            nat_16: ::icydb::__macro::CoreAsView::from_view(create.nat_16),
            nat_64: ::icydb::__macro::CoreAsView::from_view(create.nat_64),
            e8s: ::icydb::__macro::CoreAsView::from_view(create.e8s),
            e18s: ::icydb::__macro::CoreAsView::from_view(create.e18s),
            float_32: ::icydb::__macro::CoreAsView::from_view(create.float_32),
            float_64: ::icydb::__macro::CoreAsView::from_view(create.float_64),
            bool_test: ::icydb::__macro::CoreAsView::from_view(create.bool_test),
            timestamp: ::icydb::__macro::CoreAsView::from_view(create.timestamp),
            utf8_test: ::icydb::__macro::CoreAsView::from_view(create.utf8_test),
            tuple_test: ::icydb::__macro::CoreAsView::from_view(create.tuple_test),
            name_many: ::icydb::__macro::CoreAsView::from_view(create.name_many),
            name_opt: ::icydb::__macro::CoreAsView::from_view(create.name_opt),
            record_a: ::icydb::__macro::CoreAsView::from_view(create.record_a),
            record_opt: ::icydb::__macro::CoreAsView::from_view(create.record_opt),
            record_many: ::icydb::__macro::CoreAsView::from_view(create.record_many),
            list: ::icydb::__macro::CoreAsView::from_view(create.list),
            map: ::icydb::__macro::CoreAsView::from_view(create.map),
            set: ::icydb::__macro::CoreAsView::from_view(create.set),
            variant_complex: ::icydb::__macro::CoreAsView::from_view(create.variant_complex),
            variant_complex_opt: ::icydb::__macro::CoreAsView::from_view(
                create.variant_complex_opt,
            ),
            variant_complex_many: ::icydb::__macro::CoreAsView::from_view(
                create.variant_complex_many,
            ),
            variant_simple: ::icydb::__macro::CoreAsView::from_view(create.variant_simple),
            variant_simple_many: ::icydb::__macro::CoreAsView::from_view(
                create.variant_simple_many,
            ),
            variant_simple_opt: ::icydb::__macro::CoreAsView::from_view(create.variant_simple_opt),
            ..Default::default()
        }
    }
}
impl ::icydb::traits::ValidateAuto for ComplexEntity {}
impl ::icydb::traits::SanitizeCustom for ComplexEntity {}
pub mod complex_entity_views {
    #[derive(
        Debug,
        Clone,
        :: icydb :: __reexports :: serde :: Deserialize,
        :: icydb :: __reexports :: candid :: CandidType,
        :: icydb :: __reexports :: serde :: Serialize,
    )]
    pub struct ComplexEntityView {
        pub id: <::icydb::types::Ulid as ::icydb::__macro::CoreAsView>::ViewType,
        pub string_test: <::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType,
        pub principal_test: <::icydb::types::Principal as ::icydb::__macro::CoreAsView>::ViewType,
        pub blob_test: <::icydb::types::Blob as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_candid: <::icydb::types::Int as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_8: <::icydb::types::Int8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_16: <::icydb::types::Int16 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_32: <::icydb::types::Int32 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_64: <::icydb::types::Int64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_candid: <::icydb::types::Nat as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_8: <::icydb::types::Nat8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_16: <::icydb::types::Nat16 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_64: <::icydb::types::Nat64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub e8s: <super::base::types::finance::E8s as ::icydb::__macro::CoreAsView>::ViewType,
        pub e18s: <super::base::types::finance::E18s as ::icydb::__macro::CoreAsView>::ViewType,
        pub float_32: <::icydb::types::Float32 as ::icydb::__macro::CoreAsView>::ViewType,
        pub float_64: <::icydb::types::Float64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub bool_test: <::icydb::types::Bool as ::icydb::__macro::CoreAsView>::ViewType,
        pub timestamp: <::icydb::types::Timestamp as ::icydb::__macro::CoreAsView>::ViewType,
        pub utf8_test: <super::base::types::bytes::Utf8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub tuple_test: <super::Tuple as ::icydb::__macro::CoreAsView>::ViewType,
        pub name_many: Vec<<::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType>,
        pub name_opt: Option<<::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType>,
        pub record_a: <super::RecordA as ::icydb::__macro::CoreAsView>::ViewType,
        pub record_opt: Option<<super::RecordB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub record_many: Vec<<super::RecordB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub list: <super::List as ::icydb::__macro::CoreAsView>::ViewType,
        pub map: <super::Map as ::icydb::__macro::CoreAsView>::ViewType,
        pub set: <super::Set as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_complex: <super::EnumA as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_complex_opt: Option<<super::EnumA as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_complex_many: Vec<<super::EnumA as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_simple: <super::EnumB as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_simple_many: Vec<<super::EnumB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_simple_opt: Option<<super::EnumB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub created_at: <::icydb::types::Timestamp as ::icydb::__macro::CoreAsView>::ViewType,
        pub updated_at: <::icydb::types::Timestamp as ::icydb::__macro::CoreAsView>::ViewType,
    }
    impl Default for ComplexEntityView {
        fn default() -> Self {
            ::icydb::__macro::CoreAsView::as_view(&super::ComplexEntity::default())
        }
    }
    #[derive(
        :: icydb :: __reexports :: candid :: CandidType,
        Clone,
        Debug,
        :: icydb :: __reexports :: serde :: Deserialize,
        :: icydb :: __reexports :: serde :: Serialize,
    )]
    pub struct ComplexEntityCreate {
        pub string_test: <::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType,
        pub principal_test: <::icydb::types::Principal as ::icydb::__macro::CoreAsView>::ViewType,
        pub blob_test: <::icydb::types::Blob as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_candid: <::icydb::types::Int as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_8: <::icydb::types::Int8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_16: <::icydb::types::Int16 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_32: <::icydb::types::Int32 as ::icydb::__macro::CoreAsView>::ViewType,
        pub int_64: <::icydb::types::Int64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_candid: <::icydb::types::Nat as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_8: <::icydb::types::Nat8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_16: <::icydb::types::Nat16 as ::icydb::__macro::CoreAsView>::ViewType,
        pub nat_64: <::icydb::types::Nat64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub e8s: <super::base::types::finance::E8s as ::icydb::__macro::CoreAsView>::ViewType,
        pub e18s: <super::base::types::finance::E18s as ::icydb::__macro::CoreAsView>::ViewType,
        pub float_32: <::icydb::types::Float32 as ::icydb::__macro::CoreAsView>::ViewType,
        pub float_64: <::icydb::types::Float64 as ::icydb::__macro::CoreAsView>::ViewType,
        pub bool_test: <::icydb::types::Bool as ::icydb::__macro::CoreAsView>::ViewType,
        pub timestamp: <::icydb::types::Timestamp as ::icydb::__macro::CoreAsView>::ViewType,
        pub utf8_test: <super::base::types::bytes::Utf8 as ::icydb::__macro::CoreAsView>::ViewType,
        pub tuple_test: <super::Tuple as ::icydb::__macro::CoreAsView>::ViewType,
        pub name_many: Vec<<::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType>,
        pub name_opt: Option<<::icydb::types::Text as ::icydb::__macro::CoreAsView>::ViewType>,
        pub record_a: <super::RecordA as ::icydb::__macro::CoreAsView>::ViewType,
        pub record_opt: Option<<super::RecordB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub record_many: Vec<<super::RecordB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub list: <super::List as ::icydb::__macro::CoreAsView>::ViewType,
        pub map: <super::Map as ::icydb::__macro::CoreAsView>::ViewType,
        pub set: <super::Set as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_complex: <super::EnumA as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_complex_opt: Option<<super::EnumA as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_complex_many: Vec<<super::EnumA as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_simple: <super::EnumB as ::icydb::__macro::CoreAsView>::ViewType,
        pub variant_simple_many: Vec<<super::EnumB as ::icydb::__macro::CoreAsView>::ViewType>,
        pub variant_simple_opt: Option<<super::EnumB as ::icydb::__macro::CoreAsView>::ViewType>,
    }
    impl Default for ComplexEntityCreate {
        fn default() -> Self {
            let entity = super::ComplexEntity::default();
            Self {
                string_test: ::icydb::__macro::CoreAsView::as_view(&entity.string_test),
                principal_test: ::icydb::__macro::CoreAsView::as_view(&entity.principal_test),
                blob_test: ::icydb::__macro::CoreAsView::as_view(&entity.blob_test),
                int_candid: ::icydb::__macro::CoreAsView::as_view(&entity.int_candid),
                int_8: ::icydb::__macro::CoreAsView::as_view(&entity.int_8),
                int_16: ::icydb::__macro::CoreAsView::as_view(&entity.int_16),
                int_32: ::icydb::__macro::CoreAsView::as_view(&entity.int_32),
                int_64: ::icydb::__macro::CoreAsView::as_view(&entity.int_64),
                nat_candid: ::icydb::__macro::CoreAsView::as_view(&entity.nat_candid),
                nat_8: ::icydb::__macro::CoreAsView::as_view(&entity.nat_8),
                nat_16: ::icydb::__macro::CoreAsView::as_view(&entity.nat_16),
                nat_64: ::icydb::__macro::CoreAsView::as_view(&entity.nat_64),
                e8s: ::icydb::__macro::CoreAsView::as_view(&entity.e8s),
                e18s: ::icydb::__macro::CoreAsView::as_view(&entity.e18s),
                float_32: ::icydb::__macro::CoreAsView::as_view(&entity.float_32),
                float_64: ::icydb::__macro::CoreAsView::as_view(&entity.float_64),
                bool_test: ::icydb::__macro::CoreAsView::as_view(&entity.bool_test),
                timestamp: ::icydb::__macro::CoreAsView::as_view(&entity.timestamp),
                utf8_test: ::icydb::__macro::CoreAsView::as_view(&entity.utf8_test),
                tuple_test: ::icydb::__macro::CoreAsView::as_view(&entity.tuple_test),
                name_many: ::icydb::__macro::CoreAsView::as_view(&entity.name_many),
                name_opt: ::icydb::__macro::CoreAsView::as_view(&entity.name_opt),
                record_a: ::icydb::__macro::CoreAsView::as_view(&entity.record_a),
                record_opt: ::icydb::__macro::CoreAsView::as_view(&entity.record_opt),
                record_many: ::icydb::__macro::CoreAsView::as_view(&entity.record_many),
                list: ::icydb::__macro::CoreAsView::as_view(&entity.list),
                map: ::icydb::__macro::CoreAsView::as_view(&entity.map),
                set: ::icydb::__macro::CoreAsView::as_view(&entity.set),
                variant_complex: ::icydb::__macro::CoreAsView::as_view(&entity.variant_complex),
                variant_complex_opt: ::icydb::__macro::CoreAsView::as_view(
                    &entity.variant_complex_opt,
                ),
                variant_complex_many: ::icydb::__macro::CoreAsView::as_view(
                    &entity.variant_complex_many,
                ),
                variant_simple: ::icydb::__macro::CoreAsView::as_view(&entity.variant_simple),
                variant_simple_many: ::icydb::__macro::CoreAsView::as_view(
                    &entity.variant_simple_many,
                ),
                variant_simple_opt: ::icydb::__macro::CoreAsView::as_view(
                    &entity.variant_simple_opt,
                ),
            }
        }
    }
    #[derive(
        Clone,
        :: icydb :: __reexports :: serde :: Deserialize,
        Default,
        Debug,
        :: icydb :: __reexports :: candid :: CandidType,
        :: icydb :: __reexports :: serde :: Serialize,
    )]
    pub struct ComplexEntityUpdate {
        pub string_test:
            Option<<::icydb::types::Text as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub principal_test:
            Option<<::icydb::types::Principal as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub blob_test:
            Option<<::icydb::types::Blob as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub int_candid:
            Option<<::icydb::types::Int as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub int_8:
            Option<<::icydb::types::Int8 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub int_16:
            Option<<::icydb::types::Int16 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub int_32:
            Option<<::icydb::types::Int32 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub int_64:
            Option<<::icydb::types::Int64 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub nat_candid:
            Option<<::icydb::types::Nat as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub nat_8:
            Option<<::icydb::types::Nat8 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub nat_16:
            Option<<::icydb::types::Nat16 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub nat_64:
            Option<<::icydb::types::Nat64 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub e8s: Option<
            <super::base::types::finance::E8s as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
        >,
        pub e18s: Option<
            <super::base::types::finance::E18s as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
        >,
        pub float_32:
            Option<<::icydb::types::Float32 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub float_64:
            Option<<::icydb::types::Float64 as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub bool_test:
            Option<<::icydb::types::Bool as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub timestamp:
            Option<<::icydb::types::Timestamp as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub utf8_test: Option<
            <super::base::types::bytes::Utf8 as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
        >,
        pub tuple_test: Option<<super::Tuple as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub name_many: Option<
            Vec<
                ::icydb::patch::ListPatch<
                    <::icydb::types::Text as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
                >,
            >,
        >,
        pub name_opt: Option<
            Option<<::icydb::types::Text as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        >,
        pub record_a: Option<<super::RecordA as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub record_opt:
            Option<Option<<super::RecordB as ::icydb::__macro::CoreUpdateView>::UpdateViewType>>,
        pub record_many: Option<
            Vec<
                ::icydb::patch::ListPatch<
                    <super::RecordB as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
                >,
            >,
        >,
        pub list: Option<<super::List as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub map: Option<<super::Map as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub set: Option<<super::Set as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub variant_complex:
            Option<<super::EnumA as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub variant_complex_opt:
            Option<Option<<super::EnumA as ::icydb::__macro::CoreUpdateView>::UpdateViewType>>,
        pub variant_complex_many: Option<
            Vec<
                ::icydb::patch::ListPatch<
                    <super::EnumA as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
                >,
            >,
        >,
        pub variant_simple:
            Option<<super::EnumB as ::icydb::__macro::CoreUpdateView>::UpdateViewType>,
        pub variant_simple_many: Option<
            Vec<
                ::icydb::patch::ListPatch<
                    <super::EnumB as ::icydb::__macro::CoreUpdateView>::UpdateViewType,
                >,
            >,
        >,
        pub variant_simple_opt:
            Option<Option<<super::EnumB as ::icydb::__macro::CoreUpdateView>::UpdateViewType>>,
    }
}
