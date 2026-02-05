use crate::{imp::*, prelude::*, validate::entity as entity_validate};

///
/// Entity
///

#[derive(Debug, FromMeta)]
pub struct Entity {
    #[darling(default, skip)]
    pub def: Def,

    pub store: Path,

    #[darling(rename = "pk")]
    pub primary_key: Ident,

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
            .filter(|f| f.ident != self.primary_key && !f.is_system)
    }

    fn add_metadata(mut fields: FieldList) -> FieldList {
        fields.push(Field::created_at());
        fields.push(Field::updated_at());

        fields
    }
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
        let entity_name = entity_validate::validate_entity_name(self.name.as_ref(), &def_ident)?;
        entity_validate::validate_entity_indexes(&entity_name, &self.fields, &self.indexes)?;

        Ok(())
    }

    fn fatal_errors(&self) -> Vec<syn::Error> {
        let mut errors = Vec::new();
        let pk_ident = &self.primary_key;

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
                            self.primary_key
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
                    self.primary_key
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
                    self.primary_key
                ),
            ));
        }

        if pk_field.value.item.is_relation() {
            errors.push(syn::Error::new_spanned(
                pk_ident,
                format!(
                    "primary key field '{}' cannot be a relation (Ref<T> is not a primary key)",
                    self.primary_key
                ),
            ));
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
        let primary_key = quote_one(&self.primary_key, to_str_lit);
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
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::CreateView => CreateViewTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::EntityKind => EntityKindTrait::strategy(self),
            TraitKind::EntityValue => EntityValueTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => {
                // NOTE: Unsupported traits are intentionally ignored for Entity nodes.
                None
            }
        }
    }

    fn map_attribute(&self, t: TraitKind) -> Option<TokenStream> {
        match t {
            TraitKind::Default => TraitKind::Default.derive_attribute(),
            _ => {
                // NOTE: Only Default emits a derive attribute for entities.
                None
            }
        }
    }
}

impl HasType for Entity {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let primary_key = &self.primary_key;
        let fields = self.fields.iter().map(|field| {
            let field_ident = &field.ident;
            let value = if field_ident == primary_key {
                quote!(::icydb::types::Id<#ident>)
            } else {
                entity_field_type_expr(field)
            };

            quote!(pub #field_ident: #value)
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

fn entity_field_type_expr(field: &Field) -> TokenStream {
    let item = &field.value.item;
    let base = if let Some(relation) = &item.relation {
        quote!(::icydb::types::Ref<#relation>)
    } else {
        item.target().type_expr()
    };

    let base = if item.indirect {
        quote!(Box<#base>)
    } else {
        base
    };

    match field.value.cardinality() {
        Cardinality::One => quote!(#base),
        Cardinality::Opt => quote!(Option<#base>),
        Cardinality::Many => {
            if let Some(relation) = &item.relation {
                quote!(::icydb::types::RefSet<#relation>)
            } else {
                quote!(::icydb::types::OrderedList<#base>)
            }
        }
    }
}
