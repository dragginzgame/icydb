//! Module: db::access::execution_contract::pushdown
//! Defines the secondary ORDER BY pushdown matcher used by access planning and
//! execution-contract reporting.

use crate::db::{
    access::plan::{SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection},
    query::plan::DeterministicSecondaryOrderContract,
};

// Core matcher for secondary ORDER BY pushdown eligibility.
pub(in crate::db::access::execution_contract) fn match_secondary_order_pushdown_core(
    order_contract: &DeterministicSecondaryOrderContract,
    index_name: &'static str,
    index_order_terms: &[String],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let matches_expected_suffix =
        order_contract.matches_index_suffix(index_order_terms, prefix_len);
    let matches_expected_full = order_contract.matches_index_full(index_order_terms);
    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_order_terms.iter().skip(prefix_len).cloned().collect(),
            expected_full: index_order_terms.to_vec(),
            actual: order_contract.non_primary_key_terms().to_vec(),
        },
    )
}
