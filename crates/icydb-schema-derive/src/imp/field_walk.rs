use crate::prelude::*;

///
/// FieldWalkBinding
///
/// Shared derive-side lowering for one entity or record field walk step.
/// This keeps field-name, path-segment, and member-access expansion under one
/// owner so visit/sanitize/validate generation do not each rebuild that shape
/// independently.
///

pub(crate) struct FieldWalkBinding {
    ident: Ident,
    name: String,
}

impl FieldWalkBinding {
    /// Build one field-walk binding from one schema field.
    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        Self {
            ident: field.ident.clone(),
            name: field.ident.to_string(),
        }
    }

    /// Borrow this field by immutable reference from the provided receiver.
    #[must_use]
    pub fn member_ref_from(&self, receiver: TokenStream) -> TokenStream {
        let ident = &self.ident;

        quote!(&#receiver.#ident)
    }

    /// Borrow this field by mutable reference from the provided receiver.
    #[must_use]
    pub fn member_mut_from(&self, receiver: TokenStream) -> TokenStream {
        let ident = &self.ident;

        quote!(&mut #receiver.#ident)
    }

    /// Emit one field path-segment token for visitor issue reporting.
    #[must_use]
    pub fn path_segment(&self) -> TokenStream {
        let name = &self.name;

        quote!(::icydb::visitor::PathSegment::Field(#name))
    }

    /// Return the stable field name string used by visitor traversal.
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.name
    }

    /// Return one stable generated helper name for immutable traversal.
    #[must_use]
    pub fn visit_fn_ident(&self) -> Ident {
        format_ident!("__visit_field_{}", self.ident)
    }

    /// Return one stable generated helper name for mutable traversal.
    #[must_use]
    pub fn visit_mut_fn_ident(&self) -> Ident {
        format_ident!("__visit_field_{}_mut", self.ident)
    }

    /// Return one stable generated helper name for sanitization dispatch.
    #[must_use]
    pub fn sanitize_fn_ident(&self) -> Ident {
        format_ident!("__sanitize_field_{}", self.ident)
    }

    /// Return one stable generated helper name for validation dispatch.
    #[must_use]
    pub fn validate_fn_ident(&self) -> Ident {
        format_ident!("__validate_field_{}", self.ident)
    }
}

/// Lower one entity or record field list into reusable field-walk bindings.
#[must_use]
pub(crate) fn field_walk_bindings(fields: &FieldList) -> Vec<FieldWalkBinding> {
    fields.iter().map(FieldWalkBinding::from_field).collect()
}
