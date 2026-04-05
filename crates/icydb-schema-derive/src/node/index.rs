use crate::prelude::*;

///
/// Index
///

#[derive(Debug, FromMeta)]
pub struct Index {
    #[darling(default, map = "split_idents")]
    pub(crate) fields: Vec<Ident>,

    #[darling(default)]
    pub(crate) key_items: Option<LitStr>,

    #[darling(default)]
    pub(crate) unique: bool,

    #[darling(default)]
    // Raw SQL predicate text is accepted at derive input boundary and lowered
    // into canonical predicate semantics by runtime schema construction.
    pub(crate) predicate: Option<String>,
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        let fields = quote_slice(&self.fields, to_str_lit);
        let key_items = self
            .validated_key_items()
            .iter()
            .map(IndexKeyItemSpec::schema_part)
            .collect::<Vec<_>>();
        let key_items = quote! { &[#(#key_items),*] };
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
            ::icydb::schema::node::Index::new_with_key_items_and_predicate(
                #fields,
                Some(#key_items),
                #unique,
                #predicate,
            )
        }
    }
}

impl Index {
    /// Build the canonical index name (`entity|key_item|...`) shared across
    /// validation and codegen.
    pub fn generated_name(&self, entity_name: &str) -> String {
        std::iter::once(entity_name.to_string())
            .chain(self.generated_name_segments())
            .collect::<Vec<_>>()
            .join("|")
    }

    pub fn runtime_part(&self, entity_name: &str, store: &Path, ordinal: usize) -> TokenStream {
        let fields = quote_slice(&self.fields, to_str_lit);
        let key_items = self
            .validated_key_items()
            .iter()
            .map(IndexKeyItemSpec::runtime_part)
            .collect::<Vec<_>>();
        let key_items = quote! { &[#(#key_items),*] };
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
        let ordinal = u16::try_from(ordinal).expect("index ordinal should fit u16");
        let store = quote_one(store, to_path);

        // quote
        quote! {
            ::icydb::model::index::IndexModel::new_with_ordinal_and_key_items_and_predicate(
                #ordinal,
                #name,
                #store,
                #fields,
                Some(#key_items),
                #unique,
                #predicate,
            )
        }
    }

    pub(crate) fn parsed_key_items(&self) -> Result<Option<Vec<IndexKeyItemSpec>>, DarlingError> {
        self.key_items
            .as_ref()
            .map(parse_index_key_items)
            .transpose()
    }

    pub(crate) fn validated_key_item_terms(&self) -> Vec<String> {
        self.validated_key_items()
            .into_iter()
            .map(|item| item.canonical_text())
            .collect()
    }

    fn generated_name_segments(&self) -> Vec<String> {
        self.validated_key_item_terms()
    }

    fn validated_key_items(&self) -> Vec<IndexKeyItemSpec> {
        self.parsed_key_items()
            .expect("validated index key_items should parse")
            .unwrap_or_else(|| {
                self.fields
                    .iter()
                    .cloned()
                    .map(IndexKeyItemSpec::Field)
                    .collect()
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexKeyItemSpec {
    Field(Ident),
    Expression(IndexExpressionSpec),
}

impl IndexKeyItemSpec {
    pub(crate) const fn field_ident(&self) -> &Ident {
        match self {
            Self::Field(field) => field,
            Self::Expression(expression) => expression.field_ident(),
        }
    }

    fn canonical_text(&self) -> String {
        match self {
            Self::Field(field) => field.to_string(),
            Self::Expression(expression) => expression.canonical_text(),
        }
    }

    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Field(field) => {
                let field = to_str_lit(field);
                quote! { ::icydb::schema::node::IndexKeyItem::Field(#field) }
            }
            Self::Expression(expression) => {
                let expression = expression.schema_part();
                quote! { ::icydb::schema::node::IndexKeyItem::Expression(#expression) }
            }
        }
    }

    fn runtime_part(&self) -> TokenStream {
        match self {
            Self::Field(field) => {
                let field = to_str_lit(field);
                quote! { ::icydb::model::index::IndexKeyItem::Field(#field) }
            }
            Self::Expression(expression) => {
                let expression = expression.runtime_part();
                quote! { ::icydb::model::index::IndexKeyItem::Expression(#expression) }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexExpressionSpec {
    Lower(Ident),
    Upper(Ident),
    Trim(Ident),
    LowerTrim(Ident),
    Date(Ident),
    Year(Ident),
    Month(Ident),
    Day(Ident),
}

impl IndexExpressionSpec {
    const fn field_ident(&self) -> &Ident {
        match self {
            Self::Lower(field)
            | Self::Upper(field)
            | Self::Trim(field)
            | Self::LowerTrim(field)
            | Self::Date(field)
            | Self::Year(field)
            | Self::Month(field)
            | Self::Day(field) => field,
        }
    }

    fn canonical_text(&self) -> String {
        match self {
            Self::Lower(field) => format!("LOWER({field})"),
            Self::Upper(field) => format!("UPPER({field})"),
            Self::Trim(field) => format!("TRIM({field})"),
            Self::LowerTrim(field) => format!("LOWER(TRIM({field}))"),
            Self::Date(field) => format!("DATE({field})"),
            Self::Year(field) => format!("YEAR({field})"),
            Self::Month(field) => format!("MONTH({field})"),
            Self::Day(field) => format!("DAY({field})"),
        }
    }

    fn schema_part(&self) -> TokenStream {
        let field = to_str_lit(self.field_ident());

        match self {
            Self::Lower(_) => quote! { ::icydb::schema::node::IndexExpression::Lower(#field) },
            Self::Upper(_) => quote! { ::icydb::schema::node::IndexExpression::Upper(#field) },
            Self::Trim(_) => quote! { ::icydb::schema::node::IndexExpression::Trim(#field) },
            Self::LowerTrim(_) => {
                quote! { ::icydb::schema::node::IndexExpression::LowerTrim(#field) }
            }
            Self::Date(_) => quote! { ::icydb::schema::node::IndexExpression::Date(#field) },
            Self::Year(_) => quote! { ::icydb::schema::node::IndexExpression::Year(#field) },
            Self::Month(_) => quote! { ::icydb::schema::node::IndexExpression::Month(#field) },
            Self::Day(_) => quote! { ::icydb::schema::node::IndexExpression::Day(#field) },
        }
    }

    fn runtime_part(&self) -> TokenStream {
        let field = to_str_lit(self.field_ident());

        match self {
            Self::Lower(_) => quote! { ::icydb::model::index::IndexExpression::Lower(#field) },
            Self::Upper(_) => quote! { ::icydb::model::index::IndexExpression::Upper(#field) },
            Self::Trim(_) => quote! { ::icydb::model::index::IndexExpression::Trim(#field) },
            Self::LowerTrim(_) => {
                quote! { ::icydb::model::index::IndexExpression::LowerTrim(#field) }
            }
            Self::Date(_) => quote! { ::icydb::model::index::IndexExpression::Date(#field) },
            Self::Year(_) => quote! { ::icydb::model::index::IndexExpression::Year(#field) },
            Self::Month(_) => quote! { ::icydb::model::index::IndexExpression::Month(#field) },
            Self::Day(_) => quote! { ::icydb::model::index::IndexExpression::Day(#field) },
        }
    }
}

fn parse_index_key_items(literal: &LitStr) -> Result<Vec<IndexKeyItemSpec>, DarlingError> {
    let raw_items = split_top_level_key_items(literal)?;
    if raw_items.is_empty() {
        return Err(
            DarlingError::custom("index key_items must reference at least one key item")
                .with_span(literal),
        );
    }

    raw_items
        .iter()
        .map(|item| parse_index_key_item(item.as_str(), literal))
        .collect()
}

fn split_top_level_key_items(literal: &LitStr) -> Result<Vec<String>, DarlingError> {
    let raw = literal.value();
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut segment_start = 0usize;

    for (index, ch) in raw.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                if depth == 0 {
                    return Err(DarlingError::custom(format!(
                        "index key_items '{raw}' has one unmatched closing ')'"
                    ))
                    .with_span(literal));
                }
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                items.push(raw[segment_start..index].trim().to_string());
                segment_start = index.saturating_add(1);
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(DarlingError::custom(format!(
            "index key_items '{raw}' has one unmatched opening '('"
        ))
        .with_span(literal));
    }

    items.push(raw[segment_start..].trim().to_string());
    if items.iter().any(String::is_empty) {
        return Err(DarlingError::custom(format!(
            "index key_items '{raw}' contains an empty key item"
        ))
        .with_span(literal));
    }

    Ok(items)
}

fn parse_index_key_item(item: &str, literal: &LitStr) -> Result<IndexKeyItemSpec, DarlingError> {
    if let Some(expression) = parse_index_expression_item(item, literal)? {
        return Ok(IndexKeyItemSpec::Expression(expression));
    }

    let field = syn::parse_str::<Ident>(item).map_err(|_| {
        DarlingError::custom(format!(
            "unsupported index key item '{item}'; expected a field name or one supported expression form"
        ))
        .with_span(literal)
    })?;

    Ok(IndexKeyItemSpec::Field(field))
}

fn parse_index_expression_item(
    item: &str,
    literal: &LitStr,
) -> Result<Option<IndexExpressionSpec>, DarlingError> {
    if !item.contains('(') {
        return Ok(None);
    }

    if let Some(field) = parse_single_argument_function(item, "LOWER")? {
        if let Some(inner_field) = parse_single_argument_function(field, "TRIM")? {
            return Ok(Some(IndexExpressionSpec::LowerTrim(
                parse_index_field_ident(inner_field, literal)?,
            )));
        }

        return Ok(Some(IndexExpressionSpec::Lower(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "UPPER")? {
        return Ok(Some(IndexExpressionSpec::Upper(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "TRIM")? {
        return Ok(Some(IndexExpressionSpec::Trim(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "DATE")? {
        return Ok(Some(IndexExpressionSpec::Date(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "YEAR")? {
        return Ok(Some(IndexExpressionSpec::Year(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "MONTH")? {
        return Ok(Some(IndexExpressionSpec::Month(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "DAY")? {
        return Ok(Some(IndexExpressionSpec::Day(parse_index_field_ident(
            field, literal,
        )?)));
    }

    Err(
        DarlingError::custom(format!("unsupported index key item expression '{item}'"))
            .with_span(literal),
    )
}

fn parse_single_argument_function<'a>(
    input: &'a str,
    function_name: &str,
) -> Result<Option<&'a str>, DarlingError> {
    let trimmed = input.trim();
    if !trimmed.starts_with(function_name) {
        return Ok(None);
    }

    let open_index = function_name.len();
    if trimmed.as_bytes().get(open_index) != Some(&b'(') || !trimmed.ends_with(')') {
        return Err(DarlingError::custom(format!(
            "index key item expression '{trimmed}' must use canonical {function_name}(...) syntax"
        )));
    }

    Ok(Some(trimmed[open_index + 1..trimmed.len() - 1].trim()))
}

fn parse_index_field_ident(field: &str, literal: &LitStr) -> Result<Ident, DarlingError> {
    syn::parse_str::<Ident>(field).map_err(|_| {
        DarlingError::custom(format!(
            "index key item field '{field}' must be one bare field identifier"
        ))
        .with_span(literal)
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::node::index::{Index, IndexExpressionSpec, IndexKeyItemSpec};
    use proc_macro2::Span;
    use quote::format_ident;
    use syn::LitStr;

    #[test]
    fn parsed_key_items_accept_supported_expression_and_field_mix() {
        let index = Index {
            fields: vec![format_ident!("tenant_id"), format_ident!("email")],
            key_items: Some(LitStr::new("tenant_id, LOWER(email)", Span::call_site())),
            unique: true,
            predicate: None,
        };

        let key_items = index
            .parsed_key_items()
            .expect("supported index key_items should parse")
            .expect("test index should expose explicit key_items");

        assert_eq!(
            key_items,
            vec![
                IndexKeyItemSpec::Field(format_ident!("tenant_id")),
                IndexKeyItemSpec::Expression(IndexExpressionSpec::Lower(format_ident!("email"))),
            ],
        );
    }

    #[test]
    fn generated_name_uses_expression_key_item_canonical_text() {
        let index = Index {
            fields: vec![format_ident!("email")],
            key_items: Some(LitStr::new("LOWER(email)", Span::call_site())),
            unique: false,
            predicate: None,
        };

        assert_eq!(index.generated_name("User"), "User|LOWER(email)");
    }
}
