//! Module: node::list
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// List
///

#[derive(Debug, FromMeta)]
pub struct List {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    pub(crate) item: Item,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl HasDef for List {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for List {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.item.validate()?;

        Ok(())
    }
}

impl HasSchema for List {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::List
    }
}

impl HasSchemaPart for List {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let name = self.current_name_literal(self.name.as_ref());
        let item = self.item.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::List::new(#def, #name, #item, #ty)
        }
    }
}

impl HasTraits for List {
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        Some(ApplicationTypeKind::List)
    }

    fn trait_builder(&self) -> Option<&TraitBuilder> {
        Some(&self.traits)
    }

    fn trait_baseline(&self) -> TraitSet {
        let mut traits = application_type_trait_set();
        traits.extend([
            TraitKind::Default,
            TraitKind::Deref,
            TraitKind::DerefMut,
            TraitKind::From,
            TraitKind::FromIterator,
            TraitKind::IntoIterator,
        ]);

        traits
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::FromIterator => FromIteratorTrait::strategy(self),
            TraitKind::IntoIterator => IntoIteratorTrait::strategy(self),
            TraitKind::NormalizeAuto => NormalizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for List {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub Vec<#item>);
        }
    }
}

impl ToTokens for List {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let base = self.all_tokens();
        let typed_adapter = crate::node::typed_adapter::list_adapter_tokens(self);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
    }
}

#[cfg(test)]
mod tests {
    use super::List;
    use crate::prelude::*;
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    #[test]
    fn shape_baseline_precedes_trait_removal() {
        let args = NestedMeta::parse_meta_list(quote!(item(prim = "Nat8"), traits(remove(Deref))))
            .expect("list args should parse");
        let mut node = List::from_list(&args).expect("list should lower");
        node.def = Def::new(
            syn::parse2(quote!(
                pub struct WithoutDeref {}
            ))
            .expect("list input should parse as a struct"),
        );

        node.validate().expect("shape trait should be removable");
        let traits = node.traits();
        assert!(!traits.contains(&TraitKind::Deref));
        assert!(traits.contains(&TraitKind::DerefMut));
        assert!(traits.contains(&TraitKind::From));
        assert!(traits.contains(&TraitKind::FromIterator));
        assert!(traits.contains(&TraitKind::IntoIterator));
    }
}
