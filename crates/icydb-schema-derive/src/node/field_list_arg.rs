use crate::prelude::*;

pub(super) fn parse_scalar_field_arg(
    context: &str,
    expr: &syn::Expr,
) -> Result<LitStr, DarlingError> {
    let syn::Expr::Lit(expr_lit) = expr else {
        return Err(DarlingError::custom(format!(
            "{context}(field = ...) requires one string literal field name"
        ))
        .with_span(expr));
    };
    let syn::Lit::Str(literal) = &expr_lit.lit else {
        return Err(DarlingError::custom(format!(
            "{context}(field = ...) requires one string literal field name"
        ))
        .with_span(expr));
    };

    Ok(literal.clone())
}

pub(super) fn parse_field_list_arg(
    context: &str,
    expr: &syn::Expr,
) -> Result<Vec<LitStr>, DarlingError> {
    match expr {
        syn::Expr::Array(array) => array
            .elems
            .iter()
            .map(|element| {
                let syn::Expr::Lit(expr_lit) = element else {
                    return Err(DarlingError::custom(format!(
                        "{context}(fields = [...]) requires string literal field names"
                    ))
                    .with_span(element));
                };
                let syn::Lit::Str(literal) = &expr_lit.lit else {
                    return Err(DarlingError::custom(format!(
                        "{context}(fields = [...]) requires string literal field names"
                    ))
                    .with_span(element));
                };
                Ok(literal.clone())
            })
            .collect(),
        syn::Expr::Lit(expr_lit) if matches!(expr_lit.lit, syn::Lit::Str(_)) => {
            Err(DarlingError::custom(format!(
                "{context}(fields = ...) must be a Rust array literal of string literals, not a comma-string"
            ))
            .with_span(expr))
        }
        _ => Err(DarlingError::custom(format!(
            "{context}(fields = ...) must be a Rust array literal of string literals"
        ))
        .with_span(expr)),
    }
}

pub(super) fn field_or_fields_duplicate_message(context: &str) -> String {
    format!(
        "{context}(...) accepts either one field = \"...\" argument or one fields = [...] argument"
    )
}
