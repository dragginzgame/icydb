use crate::{imp::*, prelude::*};
use std::collections::HashSet;

//
// Entity
//

#[derive(Debug, FromMeta)]
pub struct Entity {
    #[darling(default, skip)]
    pub(crate) def: Def,

    pub(crate) store: Path,

    #[darling(rename = "pk")]
    pub(crate) primary_key: PrimaryKey,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(multiple, rename = "index")]
    pub(crate) indexes: Vec<Index>,

    #[darling(default, map = "Entity::add_metadata")]
    pub(crate) fields: FieldList,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl Entity {
    fn add_metadata(mut fields: FieldList) -> FieldList {
        fields.push(Field::created_at());
        fields.push(Field::updated_at());

        fields
    }

    /// Validate and resolve the effective entity name used in index naming.
    fn validate_entity_name(&self, def_ident: &Ident) -> Result<String, DarlingError> {
        // Prefer explicit user-provided names.
        if let Some(name) = self.name.as_ref() {
            let value = name.value();
            if value.len() > MAX_ENTITY_NAME_LEN {
                return Err(DarlingError::custom(format!(
                    "entity name '{value}' exceeds max length {MAX_ENTITY_NAME_LEN}"
                ))
                .with_span(name));
            }
            if !value.is_ascii() {
                return Err(
                    DarlingError::custom(format!("entity name '{value}' must be ASCII"))
                        .with_span(name),
                );
            }

            return Ok(value);
        }

        // Fall back to the Rust struct identifier.
        let value = def_ident.to_string();
        if value.len() > MAX_ENTITY_NAME_LEN {
            return Err(DarlingError::custom(format!(
                "entity name '{value}' exceeds max length {MAX_ENTITY_NAME_LEN}"
            ))
            .with_span(def_ident));
        }
        if !value.is_ascii() {
            return Err(
                DarlingError::custom(format!("entity name '{value}' must be ASCII"))
                    .with_span(def_ident),
            );
        }

        Ok(value)
    }

    /// Validate index declarations against entity fields and naming constraints.
    fn validate_indexes(&self, entity_name: &str, def_ident: &Ident) -> Result<(), DarlingError> {
        let canonical_index_terms = self.collect_canonical_index_terms(entity_name, def_ident)?;
        Self::validate_redundant_prefix_indexes(&self.indexes, &canonical_index_terms, def_ident)?;

        Ok(())
    }

    // Validate each declared index in isolation and return its canonical key terms.
    fn collect_canonical_index_terms(
        &self,
        entity_name: &str,
        def_ident: &Ident,
    ) -> Result<Vec<Vec<String>>, DarlingError> {
        let mut canonical_index_terms = Vec::with_capacity(self.indexes.len());
        for index in &self.indexes {
            Self::validate_index_shape(index, def_ident)?;
            self.validate_index_fields(index)?;
            Self::validate_index_name(index, entity_name, def_ident)?;
            self.validate_index_predicate(index)?;
            canonical_index_terms.push(index.validated_key_item_terms());
        }

        Ok(canonical_index_terms)
    }

    // Validate index cardinality limits before deeper field or expression checks.
    fn validate_index_shape(index: &Index, def_ident: &Ident) -> Result<(), DarlingError> {
        let key_items = index.parsed_key_items()?;
        if key_items.is_empty() {
            return Err(
                DarlingError::custom("index must reference at least one field")
                    .with_index_or_def_span(index, def_ident),
            );
        }
        if key_items.len() > MAX_INDEX_FIELDS {
            return Err(DarlingError::custom(format!(
                "index has {} key items; maximum is {}",
                key_items.len(),
                MAX_INDEX_FIELDS
            ))
            .with_index_or_def_span(index, def_ident));
        }

        Ok(())
    }

    // Validate declared field references against entity fields and indexability rules.
    fn validate_index_fields(&self, index: &Index) -> Result<(), DarlingError> {
        let mut seen = HashSet::new();
        for field in index.validated_field_idents() {
            let field_name = field.to_string();
            if !seen.insert(field_name.clone()) {
                return Err(DarlingError::custom(format!(
                    "index contains duplicate field '{field_name}'"
                ))
                .with_span(&index.fields));
            }

            let Some(entity_field) = self.fields.get(&field) else {
                return Err(
                    DarlingError::custom(format!("index field '{field_name}' not found"))
                        .with_span(&index.fields),
                );
            };
            if entity_field.value.cardinality() == Cardinality::Many {
                return Err(DarlingError::custom(
                    "cannot add an index field with many cardinality",
                )
                .with_span(&index.fields));
            }
        }

        Ok(())
    }

    // Validate any filtered-index predicate against the generated field surface.
    fn validate_index_predicate(&self, index: &Index) -> Result<(), DarlingError> {
        let _ = index.validated_generated_predicate(self)?;

        Ok(())
    }

    // Validate the generated runtime index name against reserved and length limits.
    fn validate_index_name(
        index: &Index,
        entity_name: &str,
        def_ident: &Ident,
    ) -> Result<(), DarlingError> {
        let index_name = index.generated_name(entity_name);
        let uses_reserved_namespace = index_name.starts_with('~')
            || index_name
                .split('|')
                .skip(1)
                .any(|segment| segment.starts_with('~'));
        if uses_reserved_namespace {
            return Err(DarlingError::custom(format!(
                "index name '{index_name}' uses reserved '~' namespace"
            ))
            .with_index_or_def_span(index, def_ident));
        }
        if index_name.len() > MAX_INDEX_NAME_LEN {
            return Err(DarlingError::custom(format!(
                "index name '{index_name}' exceeds max length {MAX_INDEX_NAME_LEN}"
            ))
            .with_index_or_def_span(index, def_ident));
        }

        Ok(())
    }

    // Reject redundant same-kind prefix indexes after each index has a canonical term list.
    fn validate_redundant_prefix_indexes(
        indexes: &[Index],
        canonical_index_terms: &[Vec<String>],
        def_ident: &Ident,
    ) -> Result<(), DarlingError> {
        for (index_idx, left_index) in indexes.iter().enumerate() {
            let left_terms = &canonical_index_terms[index_idx];
            for (right_offset, right_index) in indexes.iter().skip(index_idx + 1).enumerate() {
                let right_terms = &canonical_index_terms[index_idx + 1 + right_offset];
                if left_index.unique != right_index.unique {
                    continue;
                }

                if is_prefix_of(left_terms, right_terms) {
                    return Err(DarlingError::custom(format!(
                        "index {left_terms:?} is redundant (prefix of {right_terms:?})"
                    ))
                    .with_index_or_def_span(left_index, def_ident));
                }

                if is_prefix_of(right_terms, left_terms) {
                    return Err(DarlingError::custom(format!(
                        "index {right_terms:?} is redundant (prefix of {left_terms:?})"
                    ))
                    .with_index_or_def_span(right_index, def_ident));
                }
            }
        }

        Ok(())
    }
}

trait DarlingErrorExt {
    fn with_index_or_def_span(self, index: &Index, def_ident: &Ident) -> Self;
}

impl DarlingErrorExt for DarlingError {
    fn with_index_or_def_span(self, index: &Index, def_ident: &Ident) -> Self {
        let _ = def_ident;
        self.with_span(&index.fields)
    }
}

fn is_prefix_of(left: &[String], right: &[String]) -> bool {
    left.len() < right.len()
        && right
            .iter()
            .take(left.len())
            .zip(left.iter())
            .all(|(right_field, left_field)| right_field == left_field)
}

//
// ──────────────────────────
// TRAIT IMPLEMENTATIONS
// ──────────────────────────
//

impl HasDef for Entity {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), DarlingError> {
        // Phase 1: validate trait configuration and field shapes.
        self.traits.with_type_traits().validate()?;
        self.fields.validate()?;

        // Phase 2: validate entity name and index definitions.
        let def_ident = self.def.ident();
        let entity_name = self.validate_entity_name(&def_ident)?;
        self.validate_indexes(&entity_name, &def_ident)?;

        Ok(())
    }

    fn fatal_errors(&self) -> Vec<syn::Error> {
        let mut errors = Vec::new();
        let pk_ident = &self.primary_key.field;

        // Primary key resolution must succeed before checking shape.
        let mut pk_count = 0;
        for field in &self.fields {
            if field.ident == *pk_ident {
                pk_count += 1;
                if pk_count > 1 {
                    errors.push(syn::Error::new_spanned(
                        &field.ident,
                        format!(
                            "primary key field '{}' must appear exactly once in entity fields",
                            self.primary_key.field
                        ),
                    ));
                }
            }
        }
        if pk_count == 0 {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!(
                    "primary key field '{}' not found in entity fields",
                    self.primary_key.field
                ),
            ));
            return errors;
        }

        let Some(pk_field) = self.fields.get(pk_ident) else {
            return errors;
        };

        // Enforce primary key cardinality and relation restrictions.
        if pk_field.value.cardinality() != Cardinality::One {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!(
                    "primary key field '{}' must have cardinality One",
                    self.primary_key.field
                ),
            ));
        }

        if pk_field.value.item.is_relation() {
            // PK relation fields must declare the storage key type explicitly.
            if pk_field.value.item.primitive.is_none() {
                errors.push(syn::Error::new_spanned(
                    pk_ident,
                    format!(
                        "primary key field `{}` is a relation but has no declared primitive type; explicit prim = \"...\" is required for PK fields",
                        self.primary_key.field
                    ),
                ));
            }
        }
        if pk_field.value.item.indirect {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!(
                    "primary key field '{}' cannot use indirect item storage",
                    self.primary_key.field
                ),
            ));
        }

        match pk_field.value.item.target() {
            ItemTarget::Primitive(primitive) => {
                if !primitive.is_storage_key_encodable() {
                    errors.push(syn::Error::new_spanned(
                        pk_ident,
                        format!(
                            "primary key field '{}' must use a scalar key primitive; got '{primitive:?}'",
                            self.primary_key.field
                        ),
                    ));
                }
            }
            ItemTarget::Is(_) => {
                errors.push(syn::Error::new_spanned(
                    pk_ident,
                    format!(
                        "primary key field '{}' must declare a scalar primitive key type via \
                         prim = \"...\"; derived item(is = \"...\") types are not allowed for PKs",
                        self.primary_key.field
                    ),
                ));
            }
        }

        errors
    }
}

impl HasSchema for Entity {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Entity
    }
}

impl HasSchemaPart for Entity {
    fn schema_part(&self) -> TokenStream {
        let def = &self.def.schema_part();
        let store = quote_one(&self.store, to_path);
        let primary_key = self.primary_key.schema_part();
        let name = quote_option(self.name.as_ref(), to_str_lit);
        let indexes = quote_slice(&self.indexes, Index::schema_part);
        let fields = &self.fields.schema_part();
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            {
                const __INDEXES: &'static [::icydb::schema::node::Index] = #indexes;

                ::icydb::schema::node::Entity::new(
                    #def,
                    #store,
                    #primary_key,
                    #name,
                    __INDEXES,
                    #fields,
                    #ty,
                )
            }
        }
    }
}

impl HasTraits for Entity {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();

        traits.extend([
            TraitKind::CandidType,
            TraitKind::Inherent,
            TraitKind::EntityKind,
            TraitKind::EntityValue,
            TraitKind::FieldProjection,
            TraitKind::PersistedRow,
        ]);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::EntityKind => EntityKindTrait::strategy(self),
            TraitKind::EntityValue => EntityValueTrait::strategy(self),
            TraitKind::PersistedRow => PersistedRowTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }

    fn map_attribute(&self, t: TraitKind) -> Option<TokenStream> {
        match t {
            TraitKind::Default => TraitKind::Default.derive_attribute(),
            _ => None,
        }
    }
}

impl HasType for Entity {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let fields = self.fields.iter().map(|field| {
            let expr = field.type_expr();

            quote! {
                pub #expr
            }
        });

        quote! {
            pub struct #ident {
                #(#fields),*
            }
        }
    }
}

fn entity_create_ident(ident: &Ident) -> Ident {
    format_ident!("{ident}Create")
}

const fn field_is_insert_authorable(field: &Field) -> bool {
    field.generated.is_none() && field.write_management.is_none()
}

fn entity_create_tokens(entity: &Entity) -> TokenStream {
    let ident = entity.def.ident();
    let create_ident = entity_create_ident(&ident);
    let insert_fields = entity
        .fields
        .iter()
        .filter(|field| field_is_insert_authorable(field));
    let insert_struct_fields = insert_fields.clone().map(|field| {
        let field_ident = &field.ident;
        let field_ty = field.value.type_expr();

        quote! {
            pub #field_ident: Option<#field_ty>
        }
    });
    let insert_materialization = entity
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field_is_insert_authorable(field))
        .map(|(index, field)| {
            let field_ident = &field.ident;
            let index = syn::LitInt::new(&index.to_string(), Span::call_site());

            quote! {
                if let Some(value) = self.#field_ident {
                    entity.#field_ident = value;
                    authored_slots.push(#index);
                }
            }
        });

    quote! {
        #[doc = ""]
        #[doc = stringify!(#create_ident)]
        #[doc = ""]
        #[doc = concat!("Create-authored input for `", stringify!(#ident), "`.")]
        #[doc = "Excludes generated and managed write fields from the authored create surface."]
        #[doc = ""]
        #[derive(
            ::icydb::__reexports::candid::CandidType,
            Clone,
            Debug,
            Default,
            ::icydb::__reexports::serde::Deserialize,
            ::icydb::__reexports::serde::Serialize
        )]
        #[serde(crate = "::icydb::__reexports::serde", default)]
        pub struct #create_ident {
            #(#insert_struct_fields),*
        }

        impl ::icydb::traits::EntityCreateInput for #create_ident {
            type Entity = #ident;

            fn materialize_create(self) -> ::icydb::traits::EntityCreateMaterialization<Self::Entity> {
                let mut entity = <Self::Entity as ::core::default::Default>::default();
                let mut authored_slots = ::std::vec::Vec::new();

                #(#insert_materialization)*

                ::icydb::traits::EntityCreateMaterialization::new(entity, authored_slots)
            }
        }
    }
}

impl ToTokens for Entity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let TraitTokens { derive, impls } = self.resolve_trait_tokens();
        let schema = self.schema_tokens();
        let type_part = self.type_part();
        let insert_part = entity_create_tokens(self);

        tokens.extend(quote! {
            // SCHEMA CONSTANT
            #schema

            // MAIN TYPE
            #derive
            #type_part

            // INSERT-AUTHORED TYPE
            #insert_part

            // IMPLEMENTATIONS
            #impls
        });
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::Entity;
    use crate::node::{
        Def, Field, FieldList, Index, Item, PrimaryKey, PrimaryKeySource, Type, ValidateNode, Value,
    };
    use darling::{FromMeta, ast::NestedMeta};
    use icydb_schema::types::Primitive;
    use proc_macro2::Span;
    use quote::format_ident;
    use quote::quote;
    use syn::LitStr;

    fn scalar_field(ident: &str) -> Field {
        Field {
            ident: format_ident!("{ident}"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: None,
            generated: None,
            write_management: None,
        }
    }

    fn many_scalar_field(ident: &str) -> Field {
        Field {
            ident: format_ident!("{ident}"),
            value: Value {
                opt: false,
                many: true,
                item: Item {
                    primitive: Some(Primitive::Text),
                    ..Item::default()
                },
            },
            default: None,
            generated: None,
            write_management: None,
        }
    }

    fn entity_with_fields_and_indexes(fields: Vec<Field>, indexes: Vec<Index>) -> Entity {
        Entity {
            def: Def::new(syn::parse_quote!(
                struct TestEntity;
            )),
            store: syn::parse_quote!(UiDataStore),
            primary_key: PrimaryKey {
                field: format_ident!("id"),
                source: PrimaryKeySource::Internal,
            },
            name: None,
            indexes,
            fields: FieldList { fields },
            ty: Type::default(),
            traits: crate::trait_kind::TraitBuilder::default(),
        }
    }

    #[test]
    fn validate_rejects_index_field_not_found() {
        let entity = entity_with_fields_and_indexes(
            vec![scalar_field("id")],
            vec![Index {
                fields: LitStr::new("missing_field", Span::call_site()),
                unique: false,
                predicate: None,
            }],
        );
        let err = entity
            .validate()
            .expect_err("missing index field should fail entity validation");
        assert!(
            err.to_string()
                .contains("index field 'missing_field' not found"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn validate_rejects_many_cardinality_index_field() {
        let entity = entity_with_fields_and_indexes(
            vec![scalar_field("id"), many_scalar_field("tags")],
            vec![Index {
                fields: LitStr::new("tags", Span::call_site()),
                unique: false,
                predicate: None,
            }],
        );
        let err = entity
            .validate()
            .expect_err("indexing many-cardinality fields should fail");
        assert!(
            err.to_string()
                .contains("cannot add an index field with many cardinality"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn validate_rejects_expression_index_field_not_found() {
        let entity = entity_with_fields_and_indexes(
            vec![scalar_field("id"), scalar_field("email")],
            vec![Index {
                fields: LitStr::new("LOWER(name)", Span::call_site()),
                unique: false,
                predicate: None,
            }],
        );
        let err = entity
            .validate()
            .expect_err("missing expression index field should fail entity validation");
        assert!(
            err.to_string().contains("index field 'name' not found"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn from_list_parses_nested_indexes_and_fields() {
        let args = NestedMeta::parse_meta_list(quote!(
            store = "UiDataStore",
            pk(field = "id"),
            index(fields = "missing_field"),
            fields(field(
                ident = "id",
                value(item(prim = "Ulid")),
                default = "Ulid::generate"
            ))
        ))
        .expect("entity args should parse");

        let node = Entity::from_list(&args).expect("entity meta should lower");

        assert_eq!(
            node.indexes.len(),
            1,
            "index(...) should parse into indexes"
        );
        assert_eq!(
            node.fields.len(),
            1,
            "fields(field(...)) should parse into fields"
        );
    }
}
