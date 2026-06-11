use crate::{imp::*, prelude::*};
use icydb_core::db::{EntityName, IndexName};
use std::collections::HashSet;

//
// Entity
//

#[derive(Debug, FromMeta)]
pub struct Entity {
    #[darling(default, skip)]
    pub(crate) def: Def,

    pub(crate) store: Path,

    #[darling(rename = "version")]
    pub(crate) schema_version: u32,

    #[darling(rename = "pk")]
    pub(crate) primary_key: PrimaryKey,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(multiple, rename = "index")]
    pub(crate) indexes: Vec<Index>,

    #[darling(multiple, rename = "relation")]
    pub(crate) relations: Vec<Relation>,

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
            EntityName::try_from_str(value.as_str())
                .map_err(|err| {
                    DarlingError::custom(format!("invalid entity name '{value}': {err:?}"))
                })
                .map_err(|err| err.with_span(name))?;
            Self::validate_entity_name_namespace(value.as_str())
                .map_err(|err| err.with_span(name))?;

            return Ok(value);
        }

        // Fall back to the Rust struct identifier.
        let value = def_ident.to_string();
        EntityName::try_from_str(value.as_str())
            .map_err(|err| DarlingError::custom(format!("invalid entity name '{value}': {err:?}")))
            .map_err(|err| err.with_span(def_ident))?;
        Self::validate_entity_name_namespace(value.as_str())
            .map_err(|err| err.with_span(def_ident))?;

        Ok(value)
    }

    fn validate_entity_name_namespace(entity_name: &str) -> Result<(), DarlingError> {
        if entity_name.starts_with('~') {
            return Err(DarlingError::custom(format!(
                "entity name '{entity_name}' uses reserved '~' namespace"
            )));
        }

        Ok(())
    }

    /// Validate index declarations against entity fields and naming constraints.
    fn validate_indexes(&self, entity_name: &str, def_ident: &Ident) -> Result<(), DarlingError> {
        let canonical_index_terms = self.collect_canonical_index_terms(entity_name, def_ident)?;
        Self::validate_redundant_prefix_indexes(&self.indexes, &canonical_index_terms, def_ident)?;
        self.validate_relations()?;

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

    // Validate index cardinality before deeper field or expression checks.
    fn validate_index_shape(index: &Index, def_ident: &Ident) -> Result<(), DarlingError> {
        let key_items = index.parsed_key_items()?;
        if key_items.is_empty() {
            return Err(
                DarlingError::custom("index must reference at least one field")
                    .with_index_or_def_span(index, def_ident),
            );
        }

        Ok(())
    }

    // Validate declared field references against entity fields and indexability rules.
    fn validate_index_fields(&self, index: &Index) -> Result<(), DarlingError> {
        let mut seen = HashSet::new();
        for (field, span) in index.referenced_field_literals()? {
            let field_name = field.to_string();
            if !seen.insert(field_name.clone()) {
                return Err(DarlingError::custom(format!(
                    "index contains duplicate field '{field_name}'"
                ))
                .with_span(&span));
            }

            let Some(entity_field) = self.fields.get(&field) else {
                return Err(
                    DarlingError::custom(format!("index field '{field_name}' not found"))
                        .with_span(&span),
                );
            };
            if entity_field.value.cardinality() == Cardinality::Many {
                return Err(DarlingError::custom(
                    "cannot add an index field with many cardinality",
                )
                .with_span(&span));
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
        let entity = EntityName::try_from_str(entity_name)
            .map_err(|err| {
                DarlingError::custom(format!("invalid entity name '{entity_name}': {err:?}"))
            })
            .map_err(|err| err.with_index_or_def_span(index, def_ident))?;
        let segments = index.generated_name_segments();
        let segment_refs: Vec<&str> = segments.iter().map(String::as_str).collect();
        let index_name = if index.unique {
            IndexName::try_unique_from_entity_fields(&entity, segment_refs.as_slice())
        } else {
            IndexName::try_from_entity_fields(&entity, segment_refs.as_slice())
        }
        .map_err(|err| {
            DarlingError::custom(format!("invalid index name for '{entity_name}': {err:?}"))
        })
        .map_err(|err| err.with_index_or_def_span(index, def_ident))?;
        let index_name = index_name.as_str();
        if index_name.starts_with('~') {
            return Err(DarlingError::custom(format!(
                "index name '{index_name}' uses reserved '~' namespace"
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

    fn validate_relations(&self) -> Result<(), DarlingError> {
        for relation in &self.relations {
            relation.validate(&self.fields)?;
        }

        Ok(())
    }
}

trait DarlingErrorExt {
    fn with_index_or_def_span(self, index: &Index, def_ident: &Ident) -> Self;
}

impl DarlingErrorExt for DarlingError {
    fn with_index_or_def_span(self, index: &Index, def_ident: &Ident) -> Self {
        if let Some(field) = index.fields.first() {
            self.with_span(field)
        } else {
            self.with_span(def_ident)
        }
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
        self.validate_schema_version()?;

        // Phase 2: validate entity name and index definitions.
        let def_ident = self.def.ident();
        let entity_name = self.validate_entity_name(&def_ident)?;
        self.validate_indexes(&entity_name, &def_ident)?;

        Ok(())
    }

    fn fatal_errors(&self) -> Vec<syn::Error> {
        let mut errors = Vec::new();

        // Primary key resolution must succeed before checking each component
        // shape. The validator consumes the ordered primary-key field list so
        // scalar and composite declarations use the same component rules.
        for pk_ident in self.primary_key.fields() {
            self.collect_primary_key_field_errors(pk_ident, &mut errors);
        }

        errors
    }
}

impl Entity {
    fn collect_primary_key_field_errors(&self, pk_ident: &Ident, errors: &mut Vec<syn::Error>) {
        let mut pk_count = 0;
        for field in &self.fields {
            if field.ident == *pk_ident {
                pk_count += 1;
                if pk_count > 1 {
                    errors.push(syn::Error::new_spanned(
                        &field.ident,
                        format!(
                            "primary key field '{pk_ident}' must appear exactly once in entity fields"
                        ),
                    ));
                }
            }
        }
        if pk_count == 0 {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!("primary key field '{pk_ident}' not found in entity fields"),
            ));
            return;
        }

        let Some(pk_field) = self.fields.get(pk_ident) else {
            return;
        };

        // Enforce primary key cardinality and relation restrictions.
        if pk_field.value.cardinality() != Cardinality::One {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!("primary key field '{pk_ident}' must have cardinality One"),
            ));
        }

        if pk_field.value.item.is_relation() {
            // PK relation fields must declare the primitive key component explicitly.
            if pk_field.value.item.primitive.is_none() {
                errors.push(syn::Error::new_spanned(
                    pk_ident,
                    format!(
                        "primary key field `{pk_ident}` is a relation but has no declared primitive type; explicit prim = \"...\" is required for PK fields"
                    ),
                ));
            }
        }
        if pk_field.value.item.indirect {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!("primary key field '{pk_ident}' cannot use indirect item storage"),
            ));
        }

        match pk_field.value.item.target() {
            ItemTarget::Primitive(primitive) => {
                if self.primary_key.fields().len() > 1 && primitive == Primitive::Unit {
                    errors.push(syn::Error::new_spanned(
                        pk_ident,
                        format!(
                            "primary key field '{pk_ident}' cannot use Unit inside a composite primary key"
                        ),
                    ));
                }
                if !primitive.is_primary_key_encodable() {
                    errors.push(syn::Error::new_spanned(
                        pk_ident,
                        format!(
                            "primary key field '{pk_ident}' must use a scalar key primitive; got '{primitive:?}'"
                        ),
                    ));
                }
            }
            ItemTarget::Is(_) => {
                errors.push(syn::Error::new_spanned(
                    pk_ident,
                    format!(
                        "primary key field '{pk_ident}' must declare a scalar primitive key type via \
                         prim = \"...\"; derived item(is = \"...\") types are not allowed for PKs"
                    ),
                ));
            }
        }
    }

    fn validate_schema_version(&self) -> Result<(), DarlingError> {
        if self.schema_version == 0 {
            return Err(DarlingError::custom("version must be a positive integer")
                .with_span(&self.def.ident()));
        }

        Ok(())
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
        let schema_version = syn::LitInt::new(&self.schema_version.to_string(), Span::call_site());
        let primary_key = self.primary_key.schema_part();
        let name = quote_option(self.name.as_ref(), to_str_lit);
        let indexes = quote_slice(&self.indexes, Index::schema_part);
        let relations = quote_slice(&self.relations, Relation::schema_part);
        let fields = &self.fields.schema_part();
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            {
                const __INDEXES: &'static [::icydb::schema::node::Index] = #indexes;
                const __RELATIONS: &'static [::icydb::schema::node::RelationEdge] = #relations;

                ::icydb::schema::node::Entity::new(
                    #def,
                    #store,
                    #schema_version,
                    #primary_key,
                    #name,
                    __INDEXES,
                    __RELATIONS,
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

fn composite_primary_key_type_part(entity: &Entity) -> TokenStream {
    if entity.primary_key.fields().len() <= 1 {
        return TokenStream::new();
    }

    let key_ident = composite_primary_key_ident(&entity.def.ident());
    let key_field_specs = composite_primary_key_field_specs(entity);
    let struct_tokens = composite_primary_key_struct_tokens(&key_ident, &key_field_specs);
    let key_value_tokens = composite_primary_key_value_codec_tokens(&key_ident, &key_field_specs);
    let primary_key_codec_tokens = composite_primary_key_codec_tokens(&key_ident, &key_field_specs);
    let primary_key_decode_tokens =
        composite_primary_key_decode_tokens(&key_ident, &key_field_specs);
    let key_bytes_tokens = composite_primary_key_bytes_tokens(&key_ident, &key_field_specs);

    quote! {
        #struct_tokens
        #key_value_tokens
        #primary_key_codec_tokens
        #primary_key_decode_tokens
        #key_bytes_tokens
    }
}

fn composite_primary_key_field_specs(entity: &Entity) -> Vec<(Ident, TokenStream)> {
    entity
        .primary_key
        .fields()
        .iter()
        .map(|primary_key_field| {
            let field = entity
                .fields
                .get(primary_key_field)
                .expect("primary key field must be validated before derive generation");
            (primary_key_field.clone(), field.value.type_expr())
        })
        .collect()
}

fn composite_primary_key_struct_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let key_fields = key_field_specs.iter().map(|(primary_key_field, field_ty)| {
        quote! {
            pub #primary_key_field: #field_ty
        }
    });

    quote! {
        #[derive(
            ::icydb::__reexports::candid::CandidType,
            Clone,
            Copy,
            Debug,
            ::icydb::__reexports::serde::Deserialize,
            Eq,
            PartialEq,
            Ord,
            PartialOrd,
            Hash
        )]
        #[candid_path("::icydb::__reexports::candid")]
        #[serde(crate = "::icydb::__reexports::serde")]
        pub struct #key_ident {
            #(#key_fields),*
        }
    }
}

fn composite_primary_key_value_codec_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let component_count_lit = component_count_lit(key_field_specs);
    let key_value_encoders = key_field_specs.iter().map(|(primary_key_field, field_ty)| {
        quote! {
            <#field_ty as ::icydb::__macro::KeyValueCodec>::to_key_value(&self.#primary_key_field)
        }
    });
    let key_value_decoders =
        key_field_specs
            .iter()
            .enumerate()
            .map(|(index, (primary_key_field, field_ty))| {
                quote! {
                    #primary_key_field: <#field_ty as ::icydb::__macro::KeyValueCodec>::from_key_value(&values[#index])?
                }
            });

    quote! {
        impl ::icydb::__macro::KeyValueCodec for #key_ident {
            fn to_key_value(&self) -> ::icydb::__macro::Value {
                ::icydb::__macro::Value::List(::std::vec![
                    #(#key_value_encoders),*
                ])
            }

            fn from_key_value(value: &::icydb::__macro::Value) -> Option<Self> {
                let ::icydb::__macro::Value::List(values) = value else {
                    return None;
                };
                if values.len() != #component_count_lit {
                    return None;
                }

                Some(Self {
                    #(#key_value_decoders),*
                })
            }
        }
    }
}

fn composite_primary_key_codec_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let primary_key_component_encoders = key_field_specs.iter().map(composite_component_encoder);

    quote! {
        impl ::icydb::__macro::PrimaryKeyCodec for #key_ident {
            fn to_primary_key_value(
                &self,
            ) -> Result<
                ::icydb::__macro::PrimaryKeyValue,
                ::icydb::__macro::PrimaryKeyEncodeError,
            > {
                let components = [
                    #(#primary_key_component_encoders),*
                ];
                let composite = ::icydb::__macro::CompositePrimaryKeyValue::try_from_components(
                    &components,
                )
                .map_err(::icydb::__macro::PrimaryKeyEncodeError::from)?;

                Ok(::icydb::__macro::PrimaryKeyValue::Composite(composite))
            }
        }
    }
}

fn composite_component_encoder(
    (primary_key_field, field_ty): &(Ident, TokenStream),
) -> TokenStream {
    quote! {
        match <#field_ty as ::icydb::__macro::PrimaryKeyCodec>::to_primary_key_value(&self.#primary_key_field)? {
            ::icydb::__macro::PrimaryKeyValue::Scalar(component) => component,
            ::icydb::__macro::PrimaryKeyValue::Composite(_) => {
                return Err(::icydb::__macro::PrimaryKeyEncodeError::UnsupportedComponentKind {
                    kind: "CompositePrimaryKeyComponent",
                });
            }
        }
    }
}

fn composite_primary_key_decode_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let component_count_lit = component_count_lit(key_field_specs);
    let primary_key_component_decoders =
        key_field_specs
            .iter()
            .enumerate()
            .map(|(index, (primary_key_field, field_ty))| {
                quote! {
                    #primary_key_field: <#field_ty as ::icydb::__macro::PrimaryKeyDecode>::from_primary_key_value(
                        &::icydb::__macro::PrimaryKeyValue::Scalar(components[#index]),
                    )?
                }
            });
    quote! {
        impl ::icydb::__macro::PrimaryKeyDecode for #key_ident {
            fn from_primary_key_value(
                key: &::icydb::__macro::PrimaryKeyValue,
            ) -> Result<Self, ::icydb::__macro::InternalError> {
                let ::icydb::__macro::PrimaryKeyValue::Composite(composite) = key else {
                    return Err(::icydb::__macro::InternalError::new(
                        ::icydb::__macro::ErrorClass::Corruption,
                        ::icydb::__macro::ErrorOrigin::Store
                    ));
                };
                if composite.len() != #component_count_lit {
                    return Err(::icydb::__macro::InternalError::new(
                        ::icydb::__macro::ErrorClass::Corruption,
                        ::icydb::__macro::ErrorOrigin::Store
                    ));
                }
                let components = composite.components();

                Ok(Self {
                    #(#primary_key_component_decoders),*
                })
            }
        }
    }
}

fn composite_primary_key_bytes_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let byte_len_terms = key_field_specs
        .iter()
        .map(|(_, field_ty)| quote!(<#field_ty as ::icydb::__macro::EntityKeyBytes>::BYTE_LEN));
    let byte_writers = key_field_specs.iter().map(|(primary_key_field, field_ty)| {
        quote! {
            let (head, tail) = rest.split_at_mut(<#field_ty as ::icydb::__macro::EntityKeyBytes>::BYTE_LEN);
            <#field_ty as ::icydb::__macro::EntityKeyBytes>::write_bytes(&self.#primary_key_field, head);
            rest = tail;
        }
    });

    quote! {
        impl ::icydb::__macro::EntityKeyBytes for #key_ident {
            const BYTE_LEN: usize = 0 #( + #byte_len_terms )*;

            fn write_bytes(&self, out: &mut [u8]) {
                assert_eq!(out.len(), Self::BYTE_LEN);
                let mut rest = out;
                #(#byte_writers)*
                let _ = rest;
            }
        }
    }
}

fn component_count_lit(key_field_specs: &[(Ident, TokenStream)]) -> syn::LitInt {
    syn::LitInt::new(&key_field_specs.len().to_string(), Span::call_site())
}

fn composite_primary_key_ident(ident: &Ident) -> Ident {
    format_ident!("{ident}Key")
}

fn entity_create_ident(ident: &Ident) -> Ident {
    format_ident!("{ident}_Create")
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
    let entity_field_assignments = entity.fields.iter().enumerate().map(|(index, field)| {
        let field_ident = &field.ident;
        let index = syn::LitInt::new(&index.to_string(), Span::call_site());
        let field_name = field_ident.to_string();
        let fallback = field.rust_default_expr().unwrap_or_else(|| {
            quote! {
                return Err(::icydb::__macro::InternalError::mutation_create_missing_authored_fields(
                    <Self::Entity as ::icydb::traits::Path>::PATH,
                    #field_name,
                ))
            }
        });

        if field_is_insert_authorable(field) {
            quote! {
                #field_ident: match self.#field_ident {
                    Some(value) => {
                        authored_slots.push(#index);
                        value
                    }
                    None => #fallback,
                }
            }
        } else {
            quote!(#field_ident: #fallback)
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
            ::icydb::__reexports::serde::Deserialize
        )]
        #[candid_path("::icydb::__reexports::candid")]
        #[serde(crate = "::icydb::__reexports::serde")]
        pub struct #create_ident {
            #(#insert_struct_fields),*
        }

        impl ::icydb::traits::EntityCreateInput for #create_ident {
            type Entity = #ident;

            fn materialize_create(self) -> Result<::icydb::traits::EntityCreateMaterialization<Self::Entity>, ::icydb::__macro::InternalError> {
                let mut authored_slots = ::std::vec::Vec::new();
                let entity = #ident {
                    #(#entity_field_assignments),*
                };

                Ok(::icydb::traits::EntityCreateMaterialization::new(entity, authored_slots))
            }
        }

        impl ::icydb::traits::EntityCreateType for #ident {
            type Create = #create_ident;
        }
    }
}

impl ToTokens for Entity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let TraitTokens { derive, impls } = self.resolve_trait_tokens();
        let schema = self.schema_tokens();
        let key_part = composite_primary_key_type_part(self);
        let type_part = self.type_part();
        let insert_part = entity_create_tokens(self);

        tokens.extend(quote! {
            // SCHEMA CONSTANT
            #schema

            // PRIMARY KEY TYPE
            #key_part

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
mod tests;
