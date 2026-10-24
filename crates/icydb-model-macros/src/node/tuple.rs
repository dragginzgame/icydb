//! Module: node::tuple
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Tuple
///

#[derive(Debug, FromMeta)]
pub struct Tuple {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(multiple, rename = "value")]
    pub(crate) values: Vec<Value>,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl HasDef for Tuple {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Tuple {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;

        for value in &self.values {
            value.validate()?;
        }

        Ok(())
    }
}

impl HasSchema for Tuple {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Tuple
    }
}

impl HasSchemaPart for Tuple {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let values = quote_slice(&self.values, |value| Value::schema_part(value, paths));
        let ty = &self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::Tuple::new(#def, #name, #values, #ty)
        }
    }
}

impl HasTraits for Tuple {
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        Some(ApplicationTypeKind::Tuple)
    }

    fn trait_builder(&self) -> Option<&TraitBuilder> {
        Some(&self.traits)
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Tuple {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        let ident = self.def.ident();
        let values = self
            .values
            .iter()
            .map(|value| HasTypeExpr::type_expr(value, paths));

        quote! {
            pub struct #ident(#(pub #values),*);
        }
    }
}

impl Tuple {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let typed_adapter = crate::node::typed_adapter::tuple_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
        tokens
    }
}
