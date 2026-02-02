use proc_macro2::TokenStream;
use quote::quote;
use syn::WhereClause;

pub fn where_clause_with_bounds(
    where_clause: Option<&WhereClause>,
    bounds: &[TokenStream],
) -> TokenStream {
    if bounds.is_empty() {
        return where_clause
            .as_ref()
            .map_or_else(|| quote!(), |wc| quote!(#wc));
    }

    if let Some(wc) = where_clause {
        let preds = &wc.predicates;
        quote!(where #preds, #(#bounds),*)
    } else {
        quote!(where #(#bounds),*)
    }
}
