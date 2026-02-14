use crate::prelude::*;

///
/// Index
///

#[derive(Debug, FromMeta)]
pub struct Index {
    #[darling(default, map = "split_idents")]
    pub fields: Vec<Ident>,

    #[darling(default)]
    pub unique: bool,
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        let fields = quote_slice(&self.fields, to_str_lit);
        let unique = &self.unique;

        // quote
        quote! {
            ::icydb::schema::node::Index {
                fields: #fields,
                unique: #unique,
            }
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
        let name = LitStr::new(&self.generated_name(entity_name), Span::call_site());
        let store = quote_one(store, to_path);

        // quote
        quote! {
            ::icydb::model::index::IndexModel::new(#name, #store, #fields, #unique)
        }
    }
}
