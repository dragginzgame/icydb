//! Module: node::entity
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

#[cfg(test)]
mod tests;

use crate::{imp::*, prelude::*};
use icydb_core::db::{EntityName, IndexName};
use std::collections::HashSet;

//
// Entity
//

#[derive(Debug, FromMeta)]
#[darling(and_then = "Entity::lower_audit_timestamps")]
pub struct Entity {
    #[darling(default, skip)]
    pub(crate) def: Def,

    pub(crate) source_key: LitStr,

    pub(crate) store: Path,

    #[darling(rename = "version")]
    pub(crate) schema_version: u32,

    #[darling(rename = "pk")]
    pub(crate) primary_key: PrimaryKey,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(default)]
    pub(crate) typed_adapters: bool,

    #[darling(multiple, rename = "index")]
    pub(crate) indexes: Vec<Index>,

    #[darling(multiple, rename = "relation")]
    pub(crate) relations: Vec<Relation>,

    #[darling(multiple, rename = "constraint")]
    pub(crate) constraints: Vec<Constraint>,

    /// Parser-only shorthand consumed into ordinary managed fields before the
    /// entity can be validated or emitted.
    #[darling(default)]
    pub(crate) audit_timestamps: Option<AuditTimestamps>,

    #[darling(default)]
    pub(crate) fields: FieldList,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

/// One explicitly authored managed timestamp field.

#[derive(Clone, Debug, FromMeta)]
pub(crate) struct AuditTimestampField {
    source_key: LitStr,
    ident: Ident,
}

/// Paired authoring shorthand for the two accepted audit policies.

#[derive(Clone, Debug, FromMeta)]
pub(crate) struct AuditTimestamps {
    created_at: AuditTimestampField,
    updated_at: AuditTimestampField,
}

impl Entity {
    fn lower_audit_timestamps(mut self) -> Result<Self, DarlingError> {
        let Some(audit) = self.audit_timestamps.take() else {
            return Ok(self);
        };
        Self::reject_audit_field_collision(&self.fields, &audit.created_at)?;
        Self::reject_audit_field_collision(&self.fields, &audit.updated_at)?;
        if audit.created_at.ident == audit.updated_at.ident {
            return Err(DarlingError::custom(
                "audit timestamp fields must use distinct identifiers",
            )
            .with_span(&audit.updated_at.ident));
        }
        if audit.created_at.source_key.value() == audit.updated_at.source_key.value() {
            return Err(DarlingError::custom(
                "audit timestamp fields must use distinct source keys",
            )
            .with_span(&audit.updated_at.source_key));
        }

        self.fields.push(Field::managed_timestamp(
            audit.created_at.source_key,
            audit.created_at.ident,
            FieldWriteManagement::CreatedAt,
        ));
        self.fields.push(Field::managed_timestamp(
            audit.updated_at.source_key,
            audit.updated_at.ident,
            FieldWriteManagement::UpdatedAt,
        ));
        Ok(self)
    }

    fn reject_audit_field_collision(
        fields: &FieldList,
        audit_field: &AuditTimestampField,
    ) -> Result<(), DarlingError> {
        if fields.get(&audit_field.ident).is_some() {
            return Err(DarlingError::custom(format!(
                "audit timestamp field '{}' conflicts with an explicitly declared field",
                audit_field.ident
            ))
            .with_span(&audit_field.ident));
        }
        if fields
            .iter()
            .any(|field| field.source_key.value() == audit_field.source_key.value())
        {
            return Err(DarlingError::custom(format!(
                "audit timestamp source key '{}' conflicts with an explicitly declared field",
                audit_field.source_key.value()
            ))
            .with_span(&audit_field.source_key));
        }
        Ok(())
    }

    fn entity_name_error_text(err: impl std::fmt::Debug) -> String {
        let debug = format!("{err:?}");
        match debug.as_str() {
            "Empty" => "entity name must not be empty".to_string(),
            "NonAscii" => "entity name must be ASCII".to_string(),
            "Delimiter" => "entity name must not contain '|'".to_string(),
            _ => debug,
        }
    }

    /// Validate and resolve the effective entity name used in index naming.
    fn validate_entity_name(&self, def_ident: &Ident) -> Result<String, DarlingError> {
        // Prefer explicit user-provided names.
        if let Some(name) = self.name.as_ref() {
            let value = name.value();
            EntityName::try_from_str(value.as_str())
                .map_err(|err| Self::invalid_entity_name_error(value.as_str(), err))
                .map_err(|err| err.with_span(name))?;
            Self::validate_entity_name_namespace(value.as_str())
                .map_err(|err| err.with_span(name))?;

            return Ok(value);
        }

        // Fall back to the Rust struct identifier.
        let value = def_ident.to_string();
        EntityName::try_from_str(value.as_str())
            .map_err(|err| Self::invalid_entity_name_error(value.as_str(), err))
            .map_err(|err| err.with_span(def_ident))?;
        Self::validate_entity_name_namespace(value.as_str())
            .map_err(|err| err.with_span(def_ident))?;

        Ok(value)
    }

    fn invalid_entity_name_error(value: &str, err: impl std::fmt::Debug) -> DarlingError {
        DarlingError::custom(format!(
            "invalid entity name '{value}': {}",
            Self::entity_name_error_text(err)
        ))
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
            .map_err(|err| Self::invalid_entity_name_error(entity_name, err))
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

    fn validate_constraints(&self) -> Result<(), DarlingError> {
        let mut names = HashSet::new();
        for constraint in &self.constraints {
            if !names.insert(constraint.name.value()) {
                return Err(DarlingError::custom(format!(
                    "duplicate generated constraint name '{}'",
                    constraint.name.value()
                ))
                .with_span(&constraint.name));
            }
            let _ = constraint.validated_predicate(self)?;
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
        if self.traits.explicitly_adds(TraitKind::Default) {
            validate_struct_default_request("entity", self.def(), &self.fields)?;
        }

        // Phase 2: validate entity name and index definitions.
        let def_ident = self.def.ident();
        let entity_name = self.validate_entity_name(&def_ident)?;
        self.validate_indexes(&entity_name, &def_ident)?;
        self.validate_constraints()?;

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
        let source_key = &self.source_key;
        let store = quote_one(&self.store, to_path);
        let schema_version = syn::LitInt::new(&self.schema_version.to_string(), Span::call_site());
        let primary_key = self.primary_key.schema_part();
        let name = quote_option(self.name.as_ref(), to_str_lit);
        let entity_name = self
            .name
            .as_ref()
            .map_or_else(|| self.def.ident().to_string(), LitStr::value);
        let indexes = self
            .indexes
            .iter()
            .map(|index| index.schema_part_for_entity(self, entity_name.as_str()))
            .collect::<Result<Vec<_>, _>>()
            .expect("validated indexes should lower into source fragments");
        let indexes = quote! { &[#(#indexes),*] };
        let relations = quote_slice(&self.relations, Relation::schema_part);
        let constraints = self
            .constraints
            .iter()
            .map(|constraint| constraint.schema_part_for_entity(self))
            .collect::<Result<Vec<_>, _>>()
            .expect("validated constraints should lower into source fragments");
        let constraints = quote! { &[#(#constraints),*] };
        let fields = &self.fields.schema_part();
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            {
                const __INDEXES: &'static [::icydb::schema::node::Index] = #indexes;
                const __RELATIONS: &'static [::icydb::schema::node::RelationEdge] = #relations;
                const __CONSTRAINTS: &'static [::icydb::schema::node::CheckConstraint] = #constraints;

                ::icydb::schema::node::Entity::new(
                    #def,
                    #source_key,
                    #store,
                    #schema_version,
                    #primary_key,
                    #name,
                    __INDEXES,
                    __RELATIONS,
                    __CONSTRAINTS,
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
            TraitKind::AuthoredFieldProjection,
            TraitKind::CandidType,
            TraitKind::Inherent,
            TraitKind::EntityKind,
            TraitKind::EntityValue,
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
            TraitKind::NormalizeAuto => NormalizeAutoTrait::strategy(self),
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
    let primary_key_encode_tokens =
        composite_primary_key_encode_tokens(&key_ident, &key_field_specs);
    let primary_key_decode_tokens =
        composite_primary_key_decode_tokens(&key_ident, &key_field_specs);
    let key_bytes_tokens = composite_primary_key_bytes_tokens(&key_ident, &key_field_specs);

    quote! {
        #struct_tokens
        #key_value_tokens
        #primary_key_encode_tokens
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
    let input_value_components = key_field_specs.iter().map(
        |(primary_key_field, _)| quote!(::icydb::value::InputValue::from(value.#primary_key_field)),
    );

    quote! {
        impl From<#key_ident> for ::icydb::value::InputValue {
            fn from(value: #key_ident) -> Self {
                Self::List(::std::vec![#(#input_value_components),*])
            }
        }

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

fn composite_primary_key_encode_tokens(
    key_ident: &Ident,
    key_field_specs: &[(Ident, TokenStream)],
) -> TokenStream {
    let primary_key_component_encoders = key_field_specs.iter().map(composite_component_encoder);

    quote! {
        impl ::icydb::__macro::PrimaryKeyEncode for #key_ident {
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
        match <#field_ty as ::icydb::__macro::PrimaryKeyEncode>::to_primary_key_value(&self.#primary_key_field)? {
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
            <#field_ty as ::icydb::__macro::EntityKeyBytes>::write_bytes(&self.#primary_key_field, head)?;
            rest = tail;
        }
    });

    quote! {
        impl ::icydb::__macro::EntityKeyBytes for #key_ident {
            const BYTE_LEN: usize = 0 #( + #byte_len_terms )*;

            fn write_bytes(
                &self,
                out: &mut [u8],
            ) -> Result<(), ::icydb::__macro::EntityKeyBytesError> {
                ::icydb::__macro::validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
                let mut rest = out;
                #(#byte_writers)*
                let _ = rest;

                Ok(())
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
    let authored_field_inputs = entity
        .fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            if !field_is_insert_authorable(field) {
                return None;
            }
            let field_ident = &field.ident;
            let index = syn::LitInt::new(&index.to_string(), Span::call_site());
            let input = crate::imp::owned_value_to_input_expr(&field.value, quote!(value));

            Some(quote! {
                if let Some(value) = self.#field_ident {
                    authored_fields.push(
                        ::icydb::__macro::EntityCreateFieldInput::new(#index, #input)
                    );
                }
            })
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

        impl ::icydb::__macro::EntityCreateInput for #create_ident {
            type Entity = #ident;

            fn into_authored_fields(self) -> ::std::vec::Vec<::icydb::__macro::EntityCreateFieldInput> {
                let mut authored_fields = ::std::vec::Vec::new();
                #(#authored_field_inputs)*

                authored_fields
            }
        }

        impl ::icydb::__macro::EntityCreateType for #ident {
            type Create = #create_ident;
        }
    }
}

fn typed_adapter_input_ident(entity_ident: &Ident, suffix: &str) -> Ident {
    format_ident!("{entity_ident}{suffix}")
}

fn typed_item_field_type_tokens(item: &Item) -> TokenStream {
    let scalar = match item.target() {
        ItemTarget::Is(path) => {
            return quote! {
                ::icydb::__macro::TypedFieldType::Named(
                    <#path as ::icydb::__macro::TypedNamedType>::SOURCE_KEY,
                )
            };
        }
        ItemTarget::Primitive(primitive) => primitive,
    };
    let scalar = match scalar {
        Primitive::Account => quote!(::icydb::__macro::ScalarType::Account),
        Primitive::Blob => {
            let max_len = quote_option(item.max_len.as_ref(), |value| quote!(#value));
            quote!(::icydb::__macro::ScalarType::Blob { max_len: #max_len })
        }
        Primitive::Bool => quote!(::icydb::__macro::ScalarType::Bool),
        Primitive::Date => quote!(::icydb::__macro::ScalarType::Date),
        Primitive::Decimal => {
            let scale = item.scale.unwrap_or(0);
            quote!(::icydb::__macro::ScalarType::Decimal { scale: #scale })
        }
        Primitive::Duration => quote!(::icydb::__macro::ScalarType::Duration),
        Primitive::Float32 => quote!(::icydb::__macro::ScalarType::Float32),
        Primitive::Float64 => quote!(::icydb::__macro::ScalarType::Float64),
        Primitive::Int8 => quote!(::icydb::__macro::ScalarType::Int8),
        Primitive::Int16 => quote!(::icydb::__macro::ScalarType::Int16),
        Primitive::Int32 => quote!(::icydb::__macro::ScalarType::Int32),
        Primitive::Int64 => quote!(::icydb::__macro::ScalarType::Int64),
        Primitive::Int128 => quote!(::icydb::__macro::ScalarType::Int128),
        Primitive::IntBig => {
            let max_bytes = item.max_bytes.map_or_else(
                || quote!(::icydb::__macro::DEFAULT_BIG_INT_MAX_BYTES),
                |value| quote!(#value),
            );
            quote!(::icydb::__macro::ScalarType::IntBig { max_bytes: #max_bytes })
        }
        Primitive::Nat8 => quote!(::icydb::__macro::ScalarType::Nat8),
        Primitive::Nat16 => quote!(::icydb::__macro::ScalarType::Nat16),
        Primitive::Nat32 => quote!(::icydb::__macro::ScalarType::Nat32),
        Primitive::Nat64 => quote!(::icydb::__macro::ScalarType::Nat64),
        Primitive::Nat128 => quote!(::icydb::__macro::ScalarType::Nat128),
        Primitive::NatBig => {
            let max_bytes = item.max_bytes.map_or_else(
                || quote!(::icydb::__macro::DEFAULT_BIG_INT_MAX_BYTES),
                |value| quote!(#value),
            );
            quote!(::icydb::__macro::ScalarType::NatBig { max_bytes: #max_bytes })
        }
        Primitive::Principal => quote!(::icydb::__macro::ScalarType::Principal),
        Primitive::Subaccount => quote!(::icydb::__macro::ScalarType::Subaccount),
        Primitive::Text => {
            let max_len = quote_option(item.max_len.as_ref(), |value| quote!(#value));
            quote!(::icydb::__macro::ScalarType::Text { max_len: #max_len })
        }
        Primitive::Timestamp => quote!(::icydb::__macro::ScalarType::Timestamp),
        Primitive::Ulid => quote!(::icydb::__macro::ScalarType::Ulid),
        Primitive::Unit => quote!(::icydb::__macro::ScalarType::Unit),
    };
    quote!(::icydb::__macro::TypedFieldType::Scalar(#scalar))
}

fn typed_field_type_tokens(value: &Value) -> TokenStream {
    let item = typed_item_field_type_tokens(&value.item);
    if value.cardinality() == Cardinality::Many {
        quote!(::icydb::__macro::TypedFieldType::List(Box::new(#item)))
    } else {
        item
    }
}

fn field_is_primary_key(entity: &Entity, field: &Field) -> bool {
    entity.primary_key.fields().contains(&field.ident)
}

fn typed_write_cell_type(field: &Field) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::Opt => field.value.item.type_expr(),
        Cardinality::One | Cardinality::Many => field.value.type_expr(),
    }
}

fn typed_write_cell_input_expr(field: &Field, access: TokenStream) -> TokenStream {
    let ty = typed_write_cell_type(field);

    quote! {
        match #access {
            ::icydb::db::WriteCell::Omitted => ::icydb::db::WriteCell::Omitted,
            ::icydb::db::WriteCell::Default => ::icydb::db::WriteCell::Default,
            ::icydb::db::WriteCell::Null => ::icydb::db::WriteCell::Null,
            ::icydb::db::WriteCell::Value(value) => {
                ::icydb::db::WriteCell::Value(
                    <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                        value,
                        binding,
                    )?
                )
            }
        }
    }
}

fn typed_operation_struct_tokens(
    entity: &Entity,
    operation_ident: &Ident,
    include_primary_key: bool,
) -> TokenStream {
    let primary_key_fields = entity.fields.iter().filter_map(|field| {
        if !include_primary_key || !field_is_primary_key(entity, field) {
            return None;
        }
        let ident = &field.ident;
        let ty = field.value.type_expr();

        Some(quote!(pub #ident: #ty))
    });
    let write_fields = entity.fields.iter().filter_map(|field| {
        if !field_is_insert_authorable(field)
            || (include_primary_key && field_is_primary_key(entity, field))
        {
            return None;
        }
        let ident = &field.ident;
        let ty = typed_write_cell_type(field);

        Some(quote!(pub #ident: ::icydb::db::WriteCell<#ty>))
    });

    quote! {
        #[doc = concat!(
            "Operation-specific accepted-write intent for `",
            stringify!(#operation_ident),
            "`."
        )]
        #[doc = "Generated and managed fields are structurally absent."]
        #[derive(
            ::icydb::__reexports::candid::CandidType,
            Clone,
            Debug,
            ::icydb::__reexports::serde::Deserialize,
            Eq,
            PartialEq
        )]
        #[candid_path("::icydb::__reexports::candid")]
        #[serde(crate = "::icydb::__reexports::serde")]
        pub struct #operation_ident {
            #(#primary_key_fields,)*
            #(#write_fields),*
        }
    }
}

fn typed_primary_key_input_expr(entity: &Entity) -> TokenStream {
    if entity.primary_key.fields().len() == 1 {
        let primary_key_field = entity.primary_key.scalar_field();
        let field = entity
            .fields
            .iter()
            .find(|field| field.ident == *primary_key_field)
            .expect("validated scalar primary-key field must exist");
        let ty = field.value.type_expr();
        return quote!(
            <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                self.#primary_key_field,
                binding,
            )?
        );
    }

    let components = entity.primary_key.fields().iter().map(|field_ident| {
        let field = entity
            .fields
            .iter()
            .find(|field| field.ident == *field_ident)
            .expect("validated composite primary-key field must exist");
        let ty = field.value.type_expr();
        quote!(
            <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                self.#field_ident,
                binding,
            )?
        )
    });
    quote!(::icydb::value::InputValue::List(
        ::std::vec![#(#components),*]
    ))
}

fn typed_write_fields_tokens(entity: &Entity, include_primary_key: bool) -> Vec<TokenStream> {
    entity
        .fields
        .iter()
        .filter_map(|field| {
            if !field_is_insert_authorable(field)
                || (!include_primary_key && field_is_primary_key(entity, field))
            {
                return None;
            }
            let source_key = &field.source_key;
            let ident = &field.ident;
            let input = typed_write_cell_input_expr(field, quote!(self.#ident));

            Some(quote! {
                fields.push((#source_key, #input));
            })
        })
        .collect()
}

fn typed_write_adapter_impl_tokens(
    entity: &Entity,
    operation_ident: &Ident,
    operation: &str,
) -> TokenStream {
    let include_primary_key = operation == "insert";
    let fields = typed_write_fields_tokens(entity, include_primary_key);
    let field_count = fields.len();
    let build = match operation {
        "insert" => quote!(::icydb::db::TypedWrite::insert(binding, fields)),
        "patch" => {
            let key = typed_primary_key_input_expr(entity);
            quote!(::icydb::db::TypedWrite::update(binding, #key, fields))
        }
        "replace" => {
            let key = typed_primary_key_input_expr(entity);
            quote!(::icydb::db::TypedWrite::replace(binding, #key, fields))
        }
        _ => unreachable!("generated typed operation must be known"),
    };

    quote! {
        impl ::icydb::db::TypedWriteAdapter for #operation_ident {
            fn encode_write(
                self,
                binding: &::icydb::db::TypedEntityBinding,
            ) -> Result<::icydb::db::TypedWrite, ::icydb::db::TypedAdapterError> {
                let mut fields = ::std::vec::Vec::with_capacity(#field_count);
                #(#fields)*

                #build
            }
        }
    }
}

fn entity_typed_adapter_tokens(entity: &Entity) -> TokenStream {
    if !entity.typed_adapters {
        return TokenStream::new();
    }

    let ident = entity.def.ident();
    let insert_ident = typed_adapter_input_ident(&ident, "Insert");
    let patch_ident = typed_adapter_input_ident(&ident, "Patch");
    let replace_ident = typed_adapter_input_ident(&ident, "Replace");
    let entity_source_key = &entity.source_key;
    let field_requests = entity.fields.iter().map(|field| {
        let source_key = &field.source_key;
        let field_type = typed_field_type_tokens(&field.value);
        let nullable = field.value.cardinality() == Cardinality::Opt;
        quote! {
            ::icydb::__macro::TypedFieldBindingRequest::new(
                #source_key,
                #field_type,
                #nullable,
            )
        }
    });
    let decoded_fields = entity.fields.iter().map(|field| {
        let source_key = &field.source_key;
        let ident = &field.ident;
        let ty = field.value.type_expr();

        quote! {
            #ident: <#ty as ::icydb::__macro::TypedOutputValue>::decode_typed_output(
                binding,
                binding.row_value(#source_key, &row)?
            )?
        }
    });
    let insert_struct = typed_operation_struct_tokens(entity, &insert_ident, false);
    let patch_struct = typed_operation_struct_tokens(entity, &patch_ident, true);
    let replace_struct = typed_operation_struct_tokens(entity, &replace_ident, true);
    let insert_impl = typed_write_adapter_impl_tokens(entity, &insert_ident, "insert");
    let patch_impl = typed_write_adapter_impl_tokens(entity, &patch_ident, "patch");
    let replace_impl = typed_write_adapter_impl_tokens(entity, &replace_ident, "replace");

    quote! {
        #insert_struct
        #patch_struct
        #replace_struct

        impl #ident {
            /// Bind this generated adapter to current accepted schema authority.
            pub fn typed_binding<C>(
                session: &::icydb::db::DbSession<C>,
            ) -> Result<::icydb::db::TypedEntityBinding, ::icydb::db::TypedBindingError>
            where
                C: ::icydb::traits::CanisterKind,
            {
                session.bind_typed_entity(
                    #entity_source_key,
                    [#(#field_requests),*],
                )
            }
        }

        impl ::icydb::db::TypedRowAdapter for #ident {
            type Row = Self;

            fn decode_row(
                binding: &::icydb::db::TypedEntityBinding,
                row: ::icydb::db::OutputRow,
            ) -> Result<Self::Row, ::icydb::db::TypedAdapterError> {
                Ok(Self {
                    #(#decoded_fields),*
                })
            }
        }

        #insert_impl
        #patch_impl
        #replace_impl
    }
}

impl ToTokens for Entity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let TraitTokens { derive, impls } = self.resolve_trait_tokens();
        let schema = self.schema_tokens();
        let key_part = composite_primary_key_type_part(self);
        let type_part = self.type_part();
        let insert_part = entity_create_tokens(self);
        let typed_adapter_part = entity_typed_adapter_tokens(self);

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

            // OPTED-IN TYPED ADAPTERS
            #typed_adapter_part

            // IMPLEMENTATIONS
            #impls
        });
    }
}
