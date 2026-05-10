//! Module: query::plan::order_contract
//! Responsibility: planner-owned execution ordering contracts and direction normalization.
//! Does not own: runtime order application mechanics or cursor wire token encoding.
//! Boundary: exposes immutable order contracts consumed across planner/executor boundaries.

use crate::{
    db::{
        access::AccessCapabilities,
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec, order_term::index_order_terms},
    },
    model::index::IndexModel,
};

///
/// DeterministicSecondaryOrderContract
///
/// Planner-owned shared `..., primary_key` order contract with one uniform
/// direction. The non-primary-key term list may be empty, which represents the
/// primary-key-only order shape under the same normalized contract.
///

///
/// DeterministicSecondaryIndexOrderMatch
///
/// Planner-owned match classification between one normalized secondary ORDER BY
/// contract and one canonical index key order.
/// This exists so covering, access-contract pushdown, and planner ranking all
/// consume the same full-vs-suffix match decision instead of re-deriving it in
/// each caller.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum DeterministicSecondaryIndexOrderMatch {
    Full,
    Suffix,
    None,
}

///
/// DeterministicSecondaryIndexOrderCompatibility
///
/// Shared compatibility fact between one deterministic scalar ORDER BY
/// contract and one index-key order after a known equality-bound prefix.
/// Planner ranking and executor route pushdown both consume this value so the
/// match decision cannot drift across layers.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct DeterministicSecondaryIndexOrderCompatibility {
    index_terms: Vec<String>,
    match_kind: DeterministicSecondaryIndexOrderMatch,
}

impl DeterministicSecondaryIndexOrderCompatibility {
    /// Build one compatibility fact from the shared order contract classifier.
    #[must_use]
    fn new(
        order_contract: &DeterministicSecondaryOrderContract,
        index: &IndexModel,
        prefix_len: usize,
    ) -> Self {
        let index_terms = index_order_terms(index);
        let match_kind = order_contract.classify_index_match(&index_terms, prefix_len);

        Self {
            index_terms,
            match_kind,
        }
    }

    /// Return the full canonical index-order terms used for the match.
    #[must_use]
    pub(in crate::db) const fn index_terms(&self) -> &[String] {
        self.index_terms.as_slice()
    }

    /// Return the suffix terms remaining after the equality-bound prefix.
    #[must_use]
    pub(in crate::db) fn index_suffix_terms(&self, prefix_len: usize) -> Vec<String> {
        self.index_terms.iter().skip(prefix_len).cloned().collect()
    }

    /// Return the shared full-vs-suffix-vs-none match classification.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn match_kind(&self) -> DeterministicSecondaryIndexOrderMatch {
        self.match_kind
    }

    /// Return whether this index traversal can satisfy the ORDER BY contract.
    #[must_use]
    pub(in crate::db) const fn is_satisfied(&self) -> bool {
        !matches!(self.match_kind, DeterministicSecondaryIndexOrderMatch::None)
    }
}

///
/// GroupedIndexOrderContract
///
/// Planner-owned grouped `ORDER BY` contract without the scalar
/// `..., primary_key` tie-break normalization.
/// This exists so grouped ranking and grouped order-only fallback share one
/// full-vs-suffix index-order classifier instead of rebuilding grouped order
/// labels and uniform-direction checks in parallel.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedIndexOrderContract {
    terms: Vec<String>,
    direction: OrderDirection,
}

///
/// GroupedIndexOrderMatch
///
/// Planner-owned grouped-order match classification against one canonical
/// index key order.
/// This keeps grouped full-index and prefix-consumed suffix matching under one
/// owner instead of open-coding the same comparisons across planner helpers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedIndexOrderMatch {
    Full,
    Suffix,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct DeterministicSecondaryOrderContract {
    non_primary_key_terms: Vec<String>,
    direction: OrderDirection,
}

impl DeterministicSecondaryOrderContract {
    /// Build one normalized deterministic `..., primary_key` order contract
    /// from one executor-facing ORDER BY spec.
    #[must_use]
    pub(in crate::db) fn from_order_spec(
        order: &OrderSpec,
        primary_key_name: &str,
    ) -> Option<Self> {
        let direction = order.fields.last()?.direction();
        has_exact_primary_key_tie_break_fields(order.fields.as_slice(), primary_key_name)
            .then_some(())?;
        if order
            .fields
            .iter()
            .any(|term| term.direction() != direction)
        {
            return None;
        }

        Some(Self {
            non_primary_key_terms: order
                .fields
                .iter()
                .take(order.fields.len().saturating_sub(1))
                .map(crate::db::query::plan::OrderTerm::rendered_label)
                .collect(),
            direction,
        })
    }

    /// Return the shared direction across the full deterministic order shape.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> OrderDirection {
        self.direction
    }

    /// Borrow the normalized non-primary-key ORDER BY terms.
    #[must_use]
    pub(in crate::db) const fn non_primary_key_terms(&self) -> &[String] {
        self.non_primary_key_terms.as_slice()
    }

    /// Return true when the normalized non-primary-key terms match one expected
    /// canonical term sequence.
    #[must_use]
    pub(in crate::db) fn matches_expected_non_primary_key_terms<'a, I>(&self, expected: I) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.non_primary_key_terms
            .iter()
            .map(String::as_str)
            .eq(expected)
    }

    /// Return true when this normalized contract matches one index suffix.
    #[must_use]
    pub(in crate::db) fn matches_index_suffix<S>(
        &self,
        index_fields: &[S],
        prefix_len: usize,
    ) -> bool
    where
        S: AsRef<str>,
    {
        if prefix_len > index_fields.len() {
            return false;
        }

        self.matches_expected_non_primary_key_terms(
            index_fields[prefix_len..].iter().map(AsRef::as_ref),
        )
    }

    /// Return true when this normalized contract matches one full index order.
    #[must_use]
    pub(in crate::db) fn matches_index_full<S>(&self, index_fields: &[S]) -> bool
    where
        S: AsRef<str>,
    {
        self.matches_expected_non_primary_key_terms(index_fields.iter().map(AsRef::as_ref))
    }

    /// Classify how this normalized contract matches one canonical index key
    /// order after one equality-bound prefix.
    #[must_use]
    pub(in crate::db) fn classify_index_match<S>(
        &self,
        index_fields: &[S],
        prefix_len: usize,
    ) -> DeterministicSecondaryIndexOrderMatch
    where
        S: AsRef<str>,
    {
        if self.matches_index_suffix(index_fields, prefix_len) {
            return DeterministicSecondaryIndexOrderMatch::Suffix;
        }
        if self.matches_index_full(index_fields) {
            return DeterministicSecondaryIndexOrderMatch::Full;
        }

        DeterministicSecondaryIndexOrderMatch::None
    }
}

/// Return the shared scalar secondary-index order compatibility fact.
#[must_use]
pub(in crate::db) fn deterministic_secondary_index_order_compatibility(
    order_contract: &DeterministicSecondaryOrderContract,
    index: &IndexModel,
    prefix_len: usize,
) -> DeterministicSecondaryIndexOrderCompatibility {
    DeterministicSecondaryIndexOrderCompatibility::new(order_contract, index, prefix_len)
}

/// Return whether one deterministic scalar ORDER BY contract is satisfied by
/// one secondary-index traversal after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn deterministic_secondary_index_order_satisfied(
    order_contract: &DeterministicSecondaryOrderContract,
    index: &IndexModel,
    prefix_len: usize,
) -> bool {
    deterministic_secondary_index_order_compatibility(order_contract, index, prefix_len)
        .is_satisfied()
}

/// Return whether accepted field-path index order terms satisfy one
/// deterministic scalar ORDER BY contract after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn deterministic_secondary_index_order_terms_satisfied(
    order_contract: &DeterministicSecondaryOrderContract,
    index_terms: &[String],
    prefix_len: usize,
) -> bool {
    !matches!(
        order_contract.classify_index_match(index_terms, prefix_len),
        DeterministicSecondaryIndexOrderMatch::None
    )
}

// Empty non-unique prefix scans still interleave several leading-key groups, so
// their traversal order cannot satisfy arbitrary suffix ordering on its own.
const fn prefix_order_contract_safe(access_capabilities: &AccessCapabilities) -> bool {
    let Some(details) = access_capabilities.single_path_index_prefix_details() else {
        return false;
    };

    details.index().is_unique() || details.slot_arity() > 0
}

/// Return whether one deterministic scalar ORDER BY contract is satisfied by
/// the final stream order of one access-capability shape.
#[must_use]
pub(in crate::db) fn access_satisfies_deterministic_secondary_order_contract(
    access_capabilities: &AccessCapabilities,
    order_contract: &DeterministicSecondaryOrderContract,
) -> bool {
    if !access_capabilities.is_single_path() {
        return false;
    }

    if let Some(details) = access_capabilities.single_path_index_prefix_details() {
        return prefix_order_contract_safe(access_capabilities)
            && deterministic_secondary_index_order_satisfied(
                order_contract,
                &details.index(),
                details.slot_arity(),
            );
    }

    access_capabilities
        .single_path_index_range_details()
        .is_some_and(|details| {
            deterministic_secondary_index_order_satisfied(
                order_contract,
                &details.index(),
                details.slot_arity(),
            )
        })
}

impl GroupedIndexOrderContract {
    /// Build one grouped ORDER BY contract from one uniform-direction grouped
    /// order spec.
    #[must_use]
    pub(in crate::db) fn from_order_spec(order: &OrderSpec) -> Option<Self> {
        let direction = order
            .fields
            .first()
            .map(crate::db::query::plan::OrderTerm::direction)?;
        if order
            .fields
            .iter()
            .any(|term| term.direction() != direction)
        {
            return None;
        }

        Some(Self {
            terms: order
                .fields
                .iter()
                .map(crate::db::query::plan::OrderTerm::rendered_label)
                .collect(),
            direction,
        })
    }

    /// Return true when this grouped order matches one full canonical index
    /// order.
    #[must_use]
    pub(in crate::db) fn matches_index_full<S>(&self, index_fields: &[S]) -> bool
    where
        S: AsRef<str>,
    {
        self.terms
            .iter()
            .map(String::as_str)
            .eq(index_fields.iter().map(AsRef::as_ref))
    }

    /// Return true when this grouped order matches one canonical index suffix
    /// after one equality-bound prefix.
    #[must_use]
    pub(in crate::db) fn matches_index_suffix<S>(
        &self,
        index_fields: &[S],
        prefix_len: usize,
    ) -> bool
    where
        S: AsRef<str>,
    {
        if prefix_len > index_fields.len() {
            return false;
        }

        self.terms
            .iter()
            .map(String::as_str)
            .eq(index_fields[prefix_len..].iter().map(AsRef::as_ref))
    }

    /// Classify how this grouped order matches one canonical index key order
    /// after one equality-bound prefix.
    #[must_use]
    pub(in crate::db) fn classify_index_match<S>(
        &self,
        index_fields: &[S],
        prefix_len: usize,
    ) -> GroupedIndexOrderMatch
    where
        S: AsRef<str>,
    {
        if prefix_len > 0 && self.matches_index_suffix(index_fields, prefix_len) {
            return GroupedIndexOrderMatch::Suffix;
        }
        if self.matches_index_full(index_fields) {
            return GroupedIndexOrderMatch::Full;
        }

        GroupedIndexOrderMatch::None
    }
}

/// Return the shared grouped secondary-index order match classification.
#[must_use]
pub(in crate::db) fn grouped_index_order_match(
    order_contract: &GroupedIndexOrderContract,
    index: &IndexModel,
    prefix_len: usize,
) -> GroupedIndexOrderMatch {
    let index_terms = index_order_terms(index);

    order_contract.classify_index_match(&index_terms, prefix_len)
}

/// Return whether one grouped ORDER BY contract is satisfied by one
/// secondary-index traversal after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn grouped_index_order_satisfied(
    order_contract: &GroupedIndexOrderContract,
    index: &IndexModel,
    prefix_len: usize,
) -> bool {
    !matches!(
        grouped_index_order_match(order_contract, index, prefix_len),
        GroupedIndexOrderMatch::None
    )
}

/// Return whether accepted field-path index order terms satisfy one grouped
/// ORDER BY contract after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn grouped_index_order_terms_satisfied(
    order_contract: &GroupedIndexOrderContract,
    index_terms: &[String],
    prefix_len: usize,
) -> bool {
    !matches!(
        order_contract.classify_index_match(index_terms, prefix_len),
        GroupedIndexOrderMatch::None
    )
}

impl OrderSpec {
    /// Return the single ordered field when `ORDER BY` has exactly one element.
    #[must_use]
    pub(in crate::db) fn single_field(&self) -> Option<(&str, OrderDirection)> {
        let [term] = self.fields.as_slice() else {
            return None;
        };

        Some((term.direct_field()?, term.direction()))
    }

    /// Return ordering direction when `ORDER BY` is primary-key-only.
    #[must_use]
    pub(in crate::db) fn primary_key_only_direction(
        &self,
        primary_key_name: &str,
    ) -> Option<OrderDirection> {
        let (field, direction) = self.single_field()?;
        (field == primary_key_name).then_some(direction)
    }

    /// Return true when `ORDER BY` is exactly one primary-key field.
    #[must_use]
    pub(in crate::db) fn is_primary_key_only(&self, primary_key_name: &str) -> bool {
        self.primary_key_only_direction(primary_key_name).is_some()
    }

    /// Return true when ORDER BY includes exactly one primary-key tie-break
    /// and that tie-break is the terminal sort component.
    #[must_use]
    pub(in crate::db) fn has_exact_primary_key_tie_break(&self, primary_key_name: &str) -> bool {
        has_exact_primary_key_tie_break_fields(self.fields.as_slice(), primary_key_name)
    }

    /// Return the normalized deterministic `..., primary_key` order contract,
    /// if one exists for this ORDER BY shape.
    #[must_use]
    pub(in crate::db) fn deterministic_secondary_order_contract(
        &self,
        primary_key_name: &str,
    ) -> Option<DeterministicSecondaryOrderContract> {
        DeterministicSecondaryOrderContract::from_order_spec(self, primary_key_name)
    }

    /// Return the grouped order contract when grouped ORDER BY stays on one
    /// uniform direction.
    #[must_use]
    pub(in crate::db) fn grouped_index_order_contract(&self) -> Option<GroupedIndexOrderContract> {
        GroupedIndexOrderContract::from_order_spec(self)
    }
}

///
/// ExecutionOrdering
///
/// Planner-owned execution ordering selection.
/// Keeps scalar and grouped ordering contracts explicit at one boundary.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionOrdering {
    PrimaryKey,
    Explicit(OrderSpec),
    Grouped(Option<OrderSpec>),
}

///
/// ExecutionOrderContract
///
/// Immutable planner-projected execution ordering contract.
/// Encodes ordering shape, canonical traversal direction, and cursor support.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutionOrderContract {
    ordering: ExecutionOrdering,
    direction: Direction,
    supports_cursor: bool,
}

impl ExecutionOrderContract {
    /// Construct one immutable planner-projected execution order contract.
    #[must_use]
    const fn new(ordering: ExecutionOrdering, direction: Direction, supports_cursor: bool) -> Self {
        Self {
            ordering,
            direction,
            supports_cursor,
        }
    }

    /// Build one execution ordering contract from grouped/order plan shape.
    #[must_use]
    pub(in crate::db) fn from_plan(is_grouped: bool, order: Option<&OrderSpec>) -> Self {
        let direction = primary_scan_direction(order);
        let ordering = if is_grouped {
            ExecutionOrdering::Grouped(order.cloned())
        } else {
            match order.cloned() {
                Some(order) => ExecutionOrdering::Explicit(order),
                None => ExecutionOrdering::PrimaryKey,
            }
        };
        let supports_cursor = is_grouped || order.is_some();

        Self::new(ordering, direction, supports_cursor)
    }

    #[must_use]
    pub(in crate::db) const fn ordering(&self) -> &ExecutionOrdering {
        &self.ordering
    }

    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Return canonical primary scan direction for this execution contract.
    #[must_use]
    pub(in crate::db) const fn primary_scan_direction(&self) -> Direction {
        self.direction
    }

    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        matches!(&self.ordering, ExecutionOrdering::Grouped(_))
    }

    #[must_use]
    pub(in crate::db) const fn order_spec(&self) -> Option<&OrderSpec> {
        match &self.ordering {
            ExecutionOrdering::PrimaryKey => None,
            ExecutionOrdering::Explicit(order) => Some(order),
            ExecutionOrdering::Grouped(order) => order.as_ref(),
        }
    }
}

fn primary_scan_direction(order: Option<&OrderSpec>) -> Direction {
    let Some(order) = order else {
        return Direction::Asc;
    };
    let Some(term) = order.fields.first() else {
        return Direction::Asc;
    };

    match term.direction() {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}

fn has_exact_primary_key_tie_break_fields(
    fields: &[crate::db::query::plan::OrderTerm],
    primary_key_name: &str,
) -> bool {
    let pk_count = fields
        .iter()
        .filter(|term| term.direct_field() == Some(primary_key_name))
        .count();
    let trailing_pk = fields
        .last()
        .is_some_and(|term| term.direct_field() == Some(primary_key_name));

    pk_count == 1 && trailing_pk
}
