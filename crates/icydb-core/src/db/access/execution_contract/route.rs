//! Module: access::execution_contract::route
//! Responsibility: access-owned route classification contracts for executable access plans.
//! Does not own: executor runtime dispatch policy or planner semantic validation.
//! Boundary: exposes route capability snapshots consumed by route/executor layers.

use crate::{
    db::{
        access::{
            execution_contract::{
                ExecutableAccessPlan, pushdown::match_secondary_order_pushdown_core,
            },
            plan::{
                PushdownApplicability, SecondaryOrderPushdownEligibility,
                SecondaryOrderPushdownRejection,
            },
        },
        query::plan::{
            DeterministicSecondaryIndexOrderMatch, DeterministicSecondaryOrderContract,
            index_order_terms,
        },
    },
    model::index::IndexModel,
};

///
/// AccessRouteClass
///
/// Access-owned routing capability snapshot derived from one lowered executable
/// access plan. Router/executor policy layers consume this contract instead of
/// repeatedly branching over raw access tree structure.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db) struct AccessRouteClass {
    single_path: bool,
    composite: bool,
    range_scan: bool,
    prefix_scan: bool,
    ordered: bool,
    reverse_supported: bool,
    single_path_supports_pk_stream_access: bool,
    single_path_supports_count_pushdown_shape: bool,
    single_path_index_prefix_details: Option<(IndexModel, usize)>,
    single_path_index_range_details: Option<(IndexModel, usize)>,
    first_index_range_details: Option<(IndexModel, usize)>,
}

impl AccessRouteClass {
    #[must_use]
    pub(in crate::db) const fn single_path(self) -> bool {
        self.single_path
    }

    #[must_use]
    pub(in crate::db) const fn composite(self) -> bool {
        self.composite
    }

    #[must_use]
    pub(in crate::db) const fn range_scan(self) -> bool {
        self.range_scan
    }

    #[must_use]
    pub(in crate::db) const fn prefix_scan(self) -> bool {
        self.prefix_scan
    }

    #[must_use]
    pub(in crate::db) const fn ordered(self) -> bool {
        self.ordered
    }

    #[must_use]
    pub(in crate::db) const fn reverse_supported(self) -> bool {
        self.reverse_supported
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_pk_stream_access(self) -> bool {
        self.single_path_supports_pk_stream_access
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_count_pushdown_shape(self) -> bool {
        self.single_path_supports_count_pushdown_shape
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_prefix_details(
        self,
    ) -> Option<(IndexModel, usize)> {
        self.single_path_index_prefix_details
    }

    #[must_use]
    pub(in crate::db) const fn has_index_path(self) -> bool {
        self.single_path_index_prefix_details().is_some()
            || self.single_path_index_range_details().is_some()
    }

    #[must_use]
    pub(in crate::db) const fn prefix_order_contract_safe(self) -> bool {
        let Some((index, prefix_len)) = self.single_path_index_prefix_details() else {
            return false;
        };

        // Empty non-unique prefix scans still interleave several leading-key
        // groups, so their traversal order cannot satisfy arbitrary suffix
        // ordering on its own. Once at least one prefix slot is bound, the
        // scan is confined to one deterministic suffix window and can satisfy
        // `ORDER BY suffix..., primary_key` without a materialized sort.
        index.is_unique() || prefix_len > 0
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_range_details(
        self,
    ) -> Option<(IndexModel, usize)> {
        self.single_path_index_range_details
    }

    #[must_use]
    pub(in crate::db) const fn first_index_range_details(self) -> Option<(IndexModel, usize)> {
        self.first_index_range_details
    }

    /// Return whether one active deterministic secondary ORDER BY contract is
    /// already satisfied by this access class through one index-backed path.
    #[must_use]
    pub(in crate::db) fn index_path_satisfies_secondary_order_contract(
        self,
        order_contract: &DeterministicSecondaryOrderContract,
    ) -> bool {
        self.has_index_path()
            && (self.single_path_index_prefix_details().is_none()
                || self.prefix_order_contract_safe())
            && self
                .secondary_order_pushdown_applicability(order_contract)
                .is_eligible()
    }

    /// Derive secondary ORDER BY pushdown applicability from one access class
    /// and one planner-owned deterministic ORDER BY contract.
    #[must_use]
    pub(in crate::db) fn secondary_order_pushdown_applicability(
        self,
        order_contract: &DeterministicSecondaryOrderContract,
    ) -> PushdownApplicability {
        if !self.single_path() {
            if let Some((index, prefix_len)) = self.first_index_range_details() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                            index: index.name(),
                            prefix_len,
                        },
                    ),
                );
            }

            return PushdownApplicability::NotApplicable;
        }

        if self.prefix_scan() {
            let Some((index, prefix_len)) = self.single_path_index_prefix_details() else {
                debug_assert!(
                    false,
                    "access route class invariant: prefix-scan single-path routes must expose prefix details",
                );
                return PushdownApplicability::NotApplicable;
            };
            if prefix_len > index.fields().len() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                            prefix_len,
                            index_field_len: index.fields().len(),
                        },
                    ),
                );
            }
            let index_terms = index_order_terms(&index);

            return PushdownApplicability::Applicable(match_secondary_order_pushdown_core(
                order_contract,
                index.name(),
                &index_terms,
                prefix_len,
            ));
        }

        if self.range_scan() {
            let Some((index, prefix_len)) = self.single_path_index_range_details() else {
                debug_assert!(
                    false,
                    "access route class invariant: range-scan single-path routes must expose range details",
                );
                return PushdownApplicability::NotApplicable;
            };
            if prefix_len > index.fields().len() {
                return PushdownApplicability::Applicable(
                    SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                            prefix_len,
                            index_field_len: index.fields().len(),
                        },
                    ),
                );
            }
            let index_terms = index_order_terms(&index);

            let eligibility = match_secondary_order_pushdown_core(
                order_contract,
                index.name(),
                &index_terms,
                prefix_len,
            );
            return match eligibility {
                SecondaryOrderPushdownEligibility::Eligible { .. } => {
                    PushdownApplicability::Applicable(eligibility)
                }
                SecondaryOrderPushdownEligibility::Rejected(_) => {
                    PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
                        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                            index: index.name(),
                            prefix_len,
                        },
                    ))
                }
            };
        }

        PushdownApplicability::NotApplicable
    }

    /// Return true when this access class supports index-range limit pushdown
    /// for the supplied planner-owned deterministic ORDER BY contract.
    #[must_use]
    pub(in crate::db) fn index_range_limit_pushdown_shape_supported_for_order_contract(
        self,
        order_contract: Option<&DeterministicSecondaryOrderContract>,
        order_present: bool,
    ) -> bool {
        if !self.single_path() {
            return false;
        }
        let Some((index, prefix_len)) = self.single_path_index_range_details() else {
            return false;
        };

        if !order_present {
            return true;
        }
        let Some(order_contract) = order_contract else {
            return false;
        };
        let index_terms = index_order_terms(&index);

        matches!(
            order_contract.classify_index_match(&index_terms, prefix_len),
            DeterministicSecondaryIndexOrderMatch::Full
                | DeterministicSecondaryIndexOrderMatch::Suffix
        )
    }
}

impl<K> ExecutableAccessPlan<'_, K> {
    /// Derive one access-owned route class from this lowered executable plan.
    #[must_use]
    pub(in crate::db) fn class(&self) -> AccessRouteClass {
        // Route-class capability projection is delegated to access/capabilities.
        // This keeps route-shape predicates under one authority surface.
        let capabilities = self.capabilities();
        let single_path = capabilities.single_path();
        let single_path_index_prefix_details = single_path
            .and_then(|path| path.index_prefix_details())
            .map(|details| (details.index(), details.slot_arity()));
        let single_path_index_range_details = single_path
            .and_then(|path| path.index_range_details())
            .map(|details| (details.index(), details.slot_arity()));
        let first_index_range_details = capabilities
            .first_index_range_details()
            .map(|details| (details.index(), details.slot_arity()));

        AccessRouteClass {
            single_path: single_path.is_some(),
            composite: capabilities.is_composite(),
            range_scan: single_path_index_range_details.is_some(),
            prefix_scan: single_path_index_prefix_details.is_some(),
            ordered: true,
            reverse_supported: capabilities.all_paths_support_reverse_traversal(),
            single_path_supports_pk_stream_access: single_path
                .is_some_and(|path| path.supports_pk_stream_access()),
            single_path_supports_count_pushdown_shape: single_path
                .is_some_and(|path| path.supports_count_pushdown_shape()),
            single_path_index_prefix_details,
            single_path_index_range_details,
            first_index_range_details,
        }
    }
}
