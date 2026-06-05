//! Module: schema-check recommendations.
//! Responsibility: turn schema-check mismatch counters into user-facing guidance.
//! Does not own: schema comparison, endpoint calls, or table rendering.
//! Boundary: receives aggregate facts from analysis and returns ordered recommendation text.

pub(super) struct SchemaCheckRecommendationFacts {
    pub(super) mismatches: usize,
    pub(super) accepted_only_fields: usize,
    pub(super) accepted_ddl_indexes: usize,
    pub(super) accepted_only_generated_fields: usize,
    pub(super) generated_only_fields: usize,
    pub(super) field_default_mismatches: usize,
    pub(super) field_nullability_mismatches: usize,
    pub(super) accepted_only_generated_indexes: usize,
    pub(super) generated_only_indexes: usize,
    pub(super) index_contract_mismatches: usize,
}

pub(super) fn schema_check_recommendations(facts: &SchemaCheckRecommendationFacts) -> Vec<String> {
    let mut recommendations = Vec::new();

    if facts.mismatches > 0 {
        recommendations.push(
            "fix: resolve generated-vs-accepted mismatches before relying on schema parity"
                .to_string(),
        );
    }
    if facts.generated_only_fields > 0 {
        recommendations.push(
            "action: generated-only fields need an accepted additive transition before deploy"
                .to_string(),
        );
    }
    if facts.accepted_only_generated_fields > 0 {
        recommendations.push(
            "fix: accepted-only generated fields require an explicit retained-slot removal policy"
                .to_string(),
        );
    }
    if facts.field_default_mismatches > 0 {
        recommendations.push(
            "fix: default drift requires an explicit ALTER COLUMN SET/DROP DEFAULT flow"
                .to_string(),
        );
    }
    if facts.field_nullability_mismatches > 0 {
        recommendations.push(
            "fix: nullability drift requires an explicit ALTER COLUMN SET/DROP NOT NULL flow"
                .to_string(),
        );
    }
    if facts.generated_only_indexes > 0 {
        recommendations.push(
            "action: generated-only indexes need accepted index publication before planner parity"
                .to_string(),
        );
    }
    if facts.accepted_only_generated_indexes > 0 {
        recommendations.push(
            "fix: accepted-only generated indexes require explicit index removal or generated schema restoration"
                .to_string(),
        );
    }
    if facts.index_contract_mismatches > 0 {
        recommendations.push(
            "fix: index contract drift requires explicit index replacement, not same-name mutation"
                .to_string(),
        );
    }
    if facts.accepted_only_fields > 0 {
        recommendations.push(
            "ok: DDL-owned accepted fields are preserved catalog drift across upgrade".to_string(),
        );
        recommendations.push(
            "action: add DDL-owned fields to Rust schema only when an explicit adoption flow exists"
                .to_string(),
        );
    }
    if facts.accepted_ddl_indexes > 0 {
        recommendations.push(
            "ok: DDL-owned accepted indexes remain planner-visible catalog drift".to_string(),
        );
    }
    if recommendations.is_empty() {
        recommendations.push("ok: generated and accepted schema are aligned".to_string());
    }

    recommendations
}
