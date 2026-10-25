//! Module: node::def
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

///
/// Def
///
/// the default gets overridden after the initial darling::from_list() call
/// Schema declarations are concrete types; generic parameters are rejected before emission.
///

#[derive(Debug, Default)]
pub struct Def {
    pub(crate) item: Option<ItemStruct>,
    paths: Option<CratePaths>,
}

impl Def {
    /// Refuse authored state that generated schema types cannot preserve.
    pub(crate) fn validate_declaration(item: &ItemStruct) -> Result<(), DarlingError> {
        if !item.generics.params.is_empty() {
            return Err(DarlingError::custom(
                "schema declarations must not have generic parameters",
            )
            .with_span(&item.generics));
        }
        if !item.fields.is_empty() {
            return Err(DarlingError::custom(
                "schema declarations must have empty bodies; declare fields in the macro arguments",
            )
            .with_span(&item.fields));
        }
        Ok(())
    }

    /// Build one derive-side definition wrapper from the parsed item.
    #[must_use]
    pub const fn new(item: ItemStruct, paths: CratePaths) -> Self {
        Self {
            item: Some(item),
            paths: Some(paths),
        }
    }

    /// Preserve authored outer attributes on the main generated type.
    /// Only the macro's own debug switch is consumed instead of forwarded.
    pub(crate) fn type_attributes(&self) -> impl Iterator<Item = &syn::Attribute> {
        self.item
            .iter()
            .flat_map(|item| &item.attrs)
            .filter(|attr| !attr.path().is_ident("debug"))
    }

    /// Return the invocation paths attached before validation and emission.
    ///
    /// # Panics
    /// Panics before parsed definition metadata has been attached.
    pub(crate) const fn paths(&self) -> &CratePaths {
        self.paths
            .as_ref()
            .expect("derive definition must include resolved crate paths")
    }

    /// Return the parsed item identifier.
    ///
    /// # Panics
    ///
    /// Panics if called before darling has attached the parsed item metadata.
    pub fn ident(&self) -> Ident {
        self.item
            .as_ref()
            .unwrap_or_else(|| panic!("derive definition metadata must include the parsed item"))
            .ident
            .clone()
    }
}

impl HasSchemaPart for Def {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let ident = quote_one(&self.ident(), to_str_lit);

        // quote
        quote! {
            #model::node::Def::new(module_path!(), #ident)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Def;

    #[test]
    fn schema_declarations_reject_authored_fields_before_emission() {
        for item in [
            syn::parse_quote!(
                pub struct Empty {}
            ),
            syn::parse_quote!(
                pub struct Empty();
            ),
            syn::parse_quote!(
                pub struct Empty;
            ),
        ] {
            assert!(Def::validate_declaration(&item).is_ok());
        }
        for item in [
            syn::parse_quote!(
                pub struct Named {
                    pub note: String,
                }
            ),
            syn::parse_quote!(
                pub struct Private {
                    note: String,
                }
            ),
            syn::parse_quote!(
                pub struct Tuple(pub String, u64);
            ),
        ] {
            let error = Def::validate_declaration(&item).unwrap_err();
            assert_eq!(error.len(), 1);
            assert!(!error.write_errors().is_empty());
        }
    }

    #[test]
    fn schema_declarations_reject_generic_parameters_before_emission() {
        for item in [
            syn::parse_quote!(
                pub struct Concrete {}
            ),
            syn::parse_quote!(
                pub struct Concrete;
            ),
            syn::parse_quote!(
                pub struct Concrete
                where
                    String: Clone, {}
            ),
        ] {
            assert!(Def::validate_declaration(&item).is_ok());
        }
        for item in [
            syn::parse_quote!(
                pub struct Generic<T> {}
            ),
            syn::parse_quote!(
                pub struct Generic<'a> {}
            ),
            syn::parse_quote!(
                pub struct Generic<const N: usize> {}
            ),
            syn::parse_quote!(
                pub struct Generic<T: Clone = String> {}
            ),
            syn::parse_quote!(
                pub struct Generic<T>
                where
                    T: Clone, {}
            ),
        ] {
            let error = Def::validate_declaration(&item).unwrap_err();
            assert_eq!(error.len(), 1);
            assert!(!error.write_errors().is_empty());
        }
    }
}
