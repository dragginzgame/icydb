//! Module: node::record
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    imp::{
        DefaultTrait, NormalizeAutoTrait, ValidateAutoTrait, VisitableTrait,
        validate_struct_default_request,
    },
    prelude::*,
};

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
        let paths = self.def.paths();
        self.validate_traits()?;
        self.fields.validate(self.def.paths())?;
        if self.traits.explicitly_adds(TraitKind::Default) {
            validate_struct_default_request("record", self.def(), &self.fields, paths)?;
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
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let fields = self.fields.schema_part(paths);
        let ty = self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::Record::new(#def, #name, #fields, #ty)
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
        crate::node::traits::application_value_trait_strategy!(self, t)
    }
}

impl HasType for Record {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        self.fields.public_struct(&self.def.ident(), paths)
    }
}

impl Record {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let schema_references = if self.emit_runtime_references {
            runtime_schema_reference_tokens(&self.def, &self.fields, None, paths)
        } else {
            TokenStream::new()
        };
        let typed_adapter = crate::node::typed_adapter::record_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #schema_references
            #typed_adapter
        });
        tokens
    }
}

#[cfg(test)]
mod tests;
