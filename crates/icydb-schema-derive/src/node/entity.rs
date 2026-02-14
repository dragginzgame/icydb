use crate::{imp::*, prelude::*};
use std::collections::HashSet;

///
/// Entity
///

#[derive(Debug, FromMeta)]
pub struct Entity {
    #[darling(default, skip)]
    pub def: Def,

    pub store: Path,

    #[darling(rename = "pk")]
    pub primary_key: PrimaryKey,

    #[darling(default)]
    pub name: Option<LitStr>,

    #[darling(multiple, rename = "index")]
    pub indexes: Vec<Index>,

    #[darling(default, map = "Entity::add_metadata")]
    pub fields: FieldList,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl Entity {
    /// All user-editable fields (no PK, no system fields).
    pub fn iter_editable_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields
            .iter()
            .filter(|f| f.ident != self.primary_key.field && !f.is_system)
    }

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
        // Per-index local validation.
        for index in &self.indexes {
            if index.fields.is_empty() {
                return Err(with_index_span(
                    DarlingError::custom("index must reference at least one field"),
                    index,
                    def_ident,
                ));
            }
            if index.fields.len() > MAX_INDEX_FIELDS {
                return Err(with_index_span(
                    DarlingError::custom(format!(
                        "index has {} fields; maximum is {}",
                        index.fields.len(),
                        MAX_INDEX_FIELDS
                    )),
                    index,
                    def_ident,
                ));
            }

            // Field references must be unique, present, and indexable.
            let mut seen = HashSet::new();
            for field in &index.fields {
                let field_name = field.to_string();
                if !seen.insert(field_name.clone()) {
                    return Err(DarlingError::custom(format!(
                        "index contains duplicate field '{field_name}'"
                    ))
                    .with_span(field));
                }

                let Some(entity_field) = self.fields.get(field) else {
                    return Err(DarlingError::custom(format!(
                        "index field '{field_name}' not found"
                    ))
                    .with_span(field));
                };
                if entity_field.value.cardinality() == Cardinality::Many {
                    return Err(DarlingError::custom(
                        "cannot add an index field with many cardinality",
                    )
                    .with_span(field));
                }
            }

            // Use the same naming path as runtime index model generation.
            let index_name = index.generated_name(entity_name);
            if index_name.len() > MAX_INDEX_NAME_LEN {
                return Err(with_index_span(
                    DarlingError::custom(format!(
                        "index name '{index_name}' exceeds max length {MAX_INDEX_NAME_LEN}"
                    )),
                    index,
                    def_ident,
                ));
            }
        }

        // Cross-index validation: reject redundant same-kind prefix indexes.
        for (index_idx, left_index) in self.indexes.iter().enumerate() {
            for right_index in self.indexes.iter().skip(index_idx + 1) {
                if left_index.unique != right_index.unique {
                    continue;
                }

                if is_prefix_of(&left_index.fields, &right_index.fields) {
                    let left_fields = left_index
                        .fields
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    let right_fields = right_index
                        .fields
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    return Err(with_index_span(
                        DarlingError::custom(format!(
                            "index {left_fields:?} is redundant (prefix of {right_fields:?})"
                        )),
                        left_index,
                        def_ident,
                    ));
                }

                if is_prefix_of(&right_index.fields, &left_index.fields) {
                    let left_fields = left_index
                        .fields
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    let right_fields = right_index
                        .fields
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    return Err(with_index_span(
                        DarlingError::custom(format!(
                            "index {right_fields:?} is redundant (prefix of {left_fields:?})"
                        )),
                        right_index,
                        def_ident,
                    ));
                }
            }
        }

        Ok(())
    }
}

fn with_index_span(error: DarlingError, index: &Index, def_ident: &Ident) -> DarlingError {
    if let Some(first_field) = index.fields.first() {
        error.with_span(first_field)
    } else {
        error.with_span(def_ident)
    }
}

fn is_prefix_of(left: &[Ident], right: &[Ident]) -> bool {
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
                            "primary key field '{}' must use a scalar key primitive; got '{}'",
                            self.primary_key.field, primitive
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
             ::icydb::schema::node::Entity {
                def: #def,
                store: #store,
                primary_key: #primary_key,
                name: #name,
                indexes: #indexes,
                fields: #fields,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Entity {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();

        traits.extend(vec![
            TraitKind::Inherent,
            TraitKind::CreateView,
            TraitKind::EntityKind,
            TraitKind::EntityValue,
            TraitKind::FieldValues,
        ]);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::AsView => AsViewTrait::strategy(self),
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::CreateView => CreateViewTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::EntityKind => EntityKindTrait::strategy(self),
            TraitKind::EntityValue => EntityValueTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
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
                pub(crate) #expr
            }
        });

        quote! {
            pub struct #ident {
                #(#fields),*
            }
        }
    }
}

impl ToTokens for Entity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
