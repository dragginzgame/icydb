use crate::prelude::*;

///
/// Index
///

#[derive(Debug, FromMeta)]
pub struct Index {
    #[darling(default, map = "split_idents")]
    pub(crate) fields: Vec<Ident>,

    #[darling(default)]
    pub(crate) unique: bool,

    #[darling(default)]
    pub(crate) predicate: Option<String>,
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        let fields = quote_slice(&self.fields, to_str_lit);
        let unique = &self.unique;
        let predicate = self
            .predicate
            .as_ref()
            .map(|value| LitStr::new(value, Span::call_site()));
        let predicate = if let Some(predicate) = predicate {
            quote! { Some(#predicate) }
        } else {
            quote! { None }
        };

        // quote
        quote! {
            ::icydb::schema::node::Index::new_with_predicate(#fields, #unique, #predicate)
        }
    }
}

impl Index {
    /// Build the canonical index name (`entity|field|...`) shared across validation and codegen.
    pub fn generated_name(&self, entity_name: &str) -> String {
        std::iter::once(entity_name.to_string())
            .chain(self.fields.iter().map(ToString::to_string))
            .collect::<Vec<_>>()
            .join("|")
    }

    pub fn runtime_part(&self, entity_name: &str, store: &Path) -> TokenStream {
        let fields = quote_slice(&self.fields, to_str_lit);
        let unique = self.unique;
        let predicate = self
            .predicate
            .as_ref()
            .map(|value| LitStr::new(value, Span::call_site()));
        let predicate = if let Some(predicate) = predicate {
            quote! { Some(#predicate) }
        } else {
            quote! { None }
        };
        let name = LitStr::new(&self.generated_name(entity_name), Span::call_site());
        let store = quote_one(store, to_path);

        // quote
        quote! {
            ::icydb::model::index::IndexModel::new_with_predicate(
                #name,
                #store,
                #fields,
                #unique,
                #predicate,
            )
        }
    }
}
