use crate::prelude::*;

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
        let indexes = quote_slice(&self.indexes, Index::schema_part);
        let fields = &self.fields.schema_part();
        let ty = &self.ty.schema_part();

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Entity {
                def: #def,
                store: #store,
                primary_key: #primary_key,
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
            TraitKind::FieldValues,
            TraitKind::FilterView,
        ]);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        use crate::imp::*;

        match t {
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::CreateView => CreateViewTrait::strategy(self),
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::EntityKind => EntityKindTrait::strategy(self),
            TraitKind::FieldValues => FieldValuesTrait::strategy(self),
            TraitKind::FilterView => FilterViewTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
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
        let fields = self.fields.type_expr();

        quote! {
            pub struct #ident {
                #fields
            }
        }
    }
}

impl ToTokens for Entity {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
