use crate::{imp::*, prelude::*};

///
/// Tuple
///

#[derive(Debug, Default, FromMeta)]
pub struct Tuple {
    #[darling(default, skip)]
    pub def: Def,

    #[darling(multiple, rename = "value")]
    pub values: Vec<Value>,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for Tuple {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Tuple {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;

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
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let values = quote_slice(&self.values, Value::schema_part);
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Tuple {
                def: #def,
                values: #values,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Tuple {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();
        traits.add(TraitKind::FieldValue);
        traits.add(TraitKind::Inherent);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::View => ViewTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => {
                // NOTE: Unsupported traits are intentionally ignored for Tuple nodes.
                None
            }
        }
    }
}

impl HasType for Tuple {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let values = self.values.iter().map(HasTypeExpr::type_expr);

        quote! {
            pub struct #ident(#(pub #values),*);
        }
    }
}

impl ToTokens for Tuple {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
