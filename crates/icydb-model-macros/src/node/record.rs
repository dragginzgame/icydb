//! Module: node::record
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Record
///

#[derive(Debug, FromMeta)]
pub struct Record {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default, skip)]
    pub(crate) emit_runtime_references: bool,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(default)]
    pub(crate) fields: FieldList,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,

    #[darling(default)]
    pub(crate) ty: Type,
}

impl HasDef for Record {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Record {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.fields.validate()?;
        if self.traits.explicitly_adds(TraitKind::Default) {
            validate_struct_default_request("record", self.def(), &self.fields)?;
        }

        Ok(())
    }
}

impl HasSchema for Record {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Record
    }
}

impl HasSchemaPart for Record {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let name = self.current_name_literal(self.name.as_ref());
        let fields = self.fields.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Record::new(#def, #name, #fields, #ty)
        }
    }
}

impl HasTraits for Record {
    fn application_type_kind(&self) -> Option<ApplicationTypeKind> {
        Some(ApplicationTypeKind::Record)
    }

    fn trait_builder(&self) -> Option<&TraitBuilder> {
        Some(&self.traits)
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::NormalizeAuto => NormalizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Record {
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

impl ToTokens for Record {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let base = self.all_tokens();
        let schema_references = if self.emit_runtime_references {
            runtime_schema_reference_tokens(&self.def, &self.fields, None)
        } else {
            TokenStream::new()
        };
        let typed_adapter = crate::node::typed_adapter::record_adapter_tokens(self);
        tokens.extend(quote! {
            #base
            #schema_references
            #typed_adapter
        });
    }
}

#[cfg(test)]
mod tests {
    use super::Record;
    use crate::prelude::*;
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    #[test]
    fn unsupported_record_trait_rejects_before_emission() {
        let args = NestedMeta::parse_meta_list(quote!(traits(add(NumericValue))))
            .expect("record args should parse");
        let mut node = Record::from_list(&args).expect("record should lower");
        node.def = Def::new(
            syn::parse2(quote!(
                pub struct UnsupportedNumericRecord {}
            ))
            .expect("record input should parse as a struct"),
        );

        let error = node
            .validate()
            .expect_err("unsupported record trait should reject");
        assert!(
            error
                .to_string()
                .contains("trait 'NumericValue' is not supported by record application values"),
            "unexpected error: {error}"
        );

        let emitted_error = node.resolve_trait_tokens().impls.to_string();
        assert!(emitted_error.contains("compile_error"));
        assert!(emitted_error.contains("NumericValue"));
    }

    #[test]
    fn record_baseline_does_not_select_unemitted_from_trait() {
        let node = Record::from_list(&[]).expect("empty record should lower");

        assert!(!node.traits().contains(&TraitKind::From));
    }

    #[test]
    fn runtime_records_emit_member_references_without_an_entity_source() {
        let args = NestedMeta::parse_meta_list(quote!(fields(
            field(name = "label", value(item(prim = "Text", max_len = 32))),
            field(name = "quantity", value(item(prim = "Nat64")))
        )))
        .expect("record args should parse");
        let mut node = Record::from_list(&args).expect("record should lower");
        node.def = Def::new(
            syn::parse2(quote!(
                pub struct LineItem {}
            ))
            .expect("record input should parse as a struct"),
        );
        node.emit_runtime_references = true;

        let tokens = node.to_token_stream().to_string();

        for expected in [
            "pub const LABEL : :: icydb :: db :: query :: FieldRef",
            "FieldRef :: new (\"label\")",
            "pub const QUANTITY : :: icydb :: db :: query :: FieldRef",
            "FieldRef :: new (\"quantity\")",
        ] {
            assert!(
                tokens.contains(expected),
                "expected generated record reference `{expected}` in tokens: {tokens}",
            );
        }
        assert!(
            !tokens.contains("pub const ENTITY"),
            "record references must not claim entity identity: {tokens}",
        );
    }
}
