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
        direction::Direction,
    },
    model::{entity::EntityModel, index::IndexModel},
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
    pub(in crate::db) const fn single_path_index_range_details(
        self,
    ) -> Option<(IndexModel, usize)> {
        self.single_path_index_range_details
    }

    #[must_use]
    pub(in crate::db) const fn first_index_range_details(self) -> Option<(IndexModel, usize)> {
        self.first_index_range_details
    }

    /// Derive secondary ORDER BY pushdown applicability from one access class
    /// and normalized ORDER BY fields.
    #[must_use]
    pub(in crate::db) fn secondary_order_pushdown_applicability(
        self,
        model: &EntityModel,
        order_fields: &[(&str, Direction)],
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

            return PushdownApplicability::Applicable(match_secondary_order_pushdown_core(
                model,
                order_fields,
                index.name(),
                index.fields(),
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

            let eligibility = match_secondary_order_pushdown_core(
                model,
                order_fields,
                index.name(),
                index.fields(),
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
    /// for the supplied ORDER BY field sequence.
    #[must_use]
    pub(in crate::db) fn index_range_limit_pushdown_shape_supported_for_order<D>(
        self,
        order_fields: Option<&[(String, D)]>,
        primary_key_name: &'static str,
    ) -> bool
    where
        D: Copy + Eq,
    {
        if !self.single_path() {
            return false;
        }
        let Some((index, prefix_len)) = self.single_path_index_range_details() else {
            return false;
        };
        let index_fields = index.fields();

        let Some(order_fields) = order_fields else {
            return true;
        };
        if order_fields.is_empty() {
            return true;
        }
        let Some((_, expected_direction)) = order_fields.last() else {
            return false;
        };
        if order_fields
            .iter()
            .any(|(_, direction)| *direction != *expected_direction)
        {
            return false;
        }

        let mut expected = Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
        expected.extend(index_fields.iter().skip(prefix_len).copied());
        expected.push(primary_key_name);
        if order_fields.len() != expected.len() {
            return false;
        }
        order_fields
            .iter()
            .map(|(field, _)| field.as_str())
            .eq(expected)
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
