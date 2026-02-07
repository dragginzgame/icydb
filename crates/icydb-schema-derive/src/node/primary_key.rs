use crate::prelude::*;

///
/// PrimaryKey
///

#[derive(Debug, FromMeta)]
pub struct PrimaryKey {
    pub field: Ident,

    #[darling(default)]
    pub source: PrimaryKeySource,
}

impl HasSchemaPart for PrimaryKey {
    fn schema_part(&self) -> TokenStream {
        let field = quote_one(&self.field, to_str_lit);
        let source = self.source.schema_part();

        quote! {
            ::icydb::schema::node::PrimaryKey {
                field: #field,
                source: #source,
            }
        }
    }
}

///
/// PrimaryKeySource
///

#[derive(Clone, Copy, Debug, Default, Eq, FromMeta, PartialEq)]
pub enum PrimaryKeySource {
    #[default]
    #[darling(rename = "internal")]
    Internal,

    #[darling(rename = "external")]
    External,
}

impl HasSchemaPart for PrimaryKeySource {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Internal => quote!(::icydb::schema::node::PrimaryKeySource::Internal),
            Self::External => quote!(::icydb::schema::node::PrimaryKeySource::External),
        }
    }
}
