//! Module: query::plan::order_contract
//! Responsibility: planner-owned execution ordering contracts and direction normalization.
//! Does not own: runtime order application mechanics or cursor wire token encoding.
//! Boundary: exposes immutable order contracts consumed across planner/executor boundaries.

use crate::db::{
    access::{AccessPathKind, AccessShapeFacts, SemanticIndexKeyItem},
    direction::Direction,
    query::plan::{OrderDirection, OrderSpec, order_term::index_key_item_order_terms},
};
use std::rc::Rc;

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
        key_items: &[SemanticIndexKeyItem],
        prefix_len: usize,
    ) -> Self {
        let index_terms = index_key_item_order_terms(key_items);
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

///
/// DeterministicSecondaryOrderContract
///
/// Planner-owned shared `..., primary_key_fields` order contract with one
/// uniform direction. The non-primary-key term list may be empty, which
/// represents the primary-key-only order shape under the same normalized
/// contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct DeterministicSecondaryOrderContract {
    non_primary_key_terms: Vec<String>,
    primary_key_terms: Rc<[String]>,
    direction: OrderDirection,
}

impl DeterministicSecondaryOrderContract {
    /// Build one normalized deterministic order contract, retaining the accepted
    /// primary-key name allocation rather than copying its ordered suffix.
    #[must_use]
    pub(in crate::db) fn from_order_spec_fields(
        order: &OrderSpec,
        primary_key_names: Rc<[String]>,
    ) -> Option<Self> {
        let direction = order.fields.last()?.direction();
        has_exact_ordered_primary_key_tie_break_fields(order.fields.as_slice(), &primary_key_names)
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
                .take(order.fields.len().saturating_sub(primary_key_names.len()))
                .map(crate::db::query::plan::OrderTerm::rendered_label)
                .collect(),
            primary_key_terms: primary_key_names,
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

    /// Return whether one access-path kind requires a full index-order match.
    #[inline]
    #[must_use]
    pub(in crate::db) const fn access_kind_requires_full_index_order(
        access_kind: AccessPathKind,
    ) -> bool {
        matches!(
            access_kind,
            AccessPathKind::IndexMultiLookup | AccessPathKind::IndexBranchSet
        )
    }

    /// Return whether this order contract requires a full index-order match
    /// for the supplied variable-prefix access shape.
    #[must_use]
    pub(in crate::db) fn requires_full_index_order_for_access_shape(
        &self,
        access_shape_facts: &AccessShapeFacts,
    ) -> bool {
        if self.non_primary_key_terms.is_empty() {
            return false;
        }

        access_shape_facts
            .single_path_facts()
            .is_some_and(|path| Self::access_kind_requires_full_index_order(path.kind()))
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
        self.classify_index_match_by(index_fields.len(), prefix_len, |index, term| {
            index_fields[index].as_ref() == term
        })
    }

    /// Classify accepted key items without rendering a temporary label list.
    #[must_use]
    pub(in crate::db) fn classify_index_key_items(
        &self,
        key_items: &[SemanticIndexKeyItem],
        prefix_len: usize,
    ) -> DeterministicSecondaryIndexOrderMatch {
        self.classify_index_match_by(key_items.len(), prefix_len, |index, term| {
            key_items[index].as_ref().matches_canonical_text(term)
        })
    }

    // Both retained diagnostic labels and borrowed accepted keys use this one
    // suffix-removal and match-precedence authority. Normalize the PK tail once.
    fn classify_index_match_by(
        &self,
        mut index_len: usize,
        prefix_len: usize,
        matches: impl Fn(usize, &str) -> bool,
    ) -> DeterministicSecondaryIndexOrderMatch {
        let suffix_len = self.primary_key_terms.len();
        if suffix_len > 0
            && suffix_len <= index_len
            && order_terms_match_at(
                &self.primary_key_terms,
                index_len,
                index_len - suffix_len,
                &matches,
            )
        {
            index_len -= suffix_len;
        }
        let terms = &self.non_primary_key_terms;
        if prefix_len <= index_len
            && terms.len() == index_len - prefix_len
            && order_terms_match_at(terms, index_len, prefix_len, &matches)
        {
            return DeterministicSecondaryIndexOrderMatch::Suffix;
        }
        if terms.len() == index_len && order_terms_match_at(terms, index_len, 0, &matches) {
            return DeterministicSecondaryIndexOrderMatch::Full;
        }
        DeterministicSecondaryIndexOrderMatch::None
    }
}

/// Return the shared scalar secondary-index order compatibility fact from
/// reduced key-item facts.
#[must_use]
pub(in crate::db) fn deterministic_secondary_index_key_items_order_compatibility(
    order_contract: &DeterministicSecondaryOrderContract,
    key_items: &[SemanticIndexKeyItem],
    prefix_len: usize,
) -> DeterministicSecondaryIndexOrderCompatibility {
    DeterministicSecondaryIndexOrderCompatibility::new(order_contract, key_items, prefix_len)
}

/// Return whether accepted field-path index order terms satisfy one
/// deterministic scalar ORDER BY contract after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn deterministic_secondary_index_key_items_satisfied(
    order_contract: &DeterministicSecondaryOrderContract,
    key_items: &[SemanticIndexKeyItem],
    prefix_len: usize,
) -> bool {
    !matches!(
        order_contract.classify_index_key_items(key_items, prefix_len),
        DeterministicSecondaryIndexOrderMatch::None
    )
}

// Empty non-unique prefix scans still interleave several leading-key groups, so
// their traversal order cannot satisfy arbitrary suffix ordering on its own.
fn prefix_order_contract_safe(access_shape_facts: &AccessShapeFacts) -> bool {
    let Some(details) = access_shape_facts.single_path_index_prefix_details() else {
        return false;
    };

    details.is_unique() || details.slot_arity() > 0
}

fn deterministic_secondary_index_key_items_order_satisfied_for_access_shape(
    access_shape_facts: &AccessShapeFacts,
    order_contract: &DeterministicSecondaryOrderContract,
    key_items: &[SemanticIndexKeyItem],
    prefix_len: usize,
) -> bool {
    match order_contract.classify_index_key_items(key_items, prefix_len) {
        DeterministicSecondaryIndexOrderMatch::Full => true,
        DeterministicSecondaryIndexOrderMatch::Suffix => {
            !order_contract.requires_full_index_order_for_access_shape(access_shape_facts)
        }
        DeterministicSecondaryIndexOrderMatch::None => false,
    }
}

/// Return whether one deterministic scalar ORDER BY contract is satisfied by
/// the final stream order of one access-capability shape.
#[must_use]
pub(in crate::db) fn access_satisfies_deterministic_secondary_order_contract(
    access_shape_facts: &AccessShapeFacts,
    order_contract: &DeterministicSecondaryOrderContract,
) -> bool {
    if !access_shape_facts.is_single_path() {
        return false;
    }

    if let Some(details) = access_shape_facts.single_path_index_prefix_details() {
        return prefix_order_contract_safe(access_shape_facts)
            && deterministic_secondary_index_key_items_order_satisfied_for_access_shape(
                access_shape_facts,
                order_contract,
                details.key_items(),
                details.slot_arity(),
            );
    }

    access_shape_facts
        .single_path_index_range_details()
        .is_some_and(|details| {
            deterministic_secondary_index_key_items_order_satisfied_for_access_shape(
                access_shape_facts,
                order_contract,
                details.key_items(),
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

    /// Classify accepted key items without rendering a temporary label list.
    #[must_use]
    pub(in crate::db) fn classify_index_key_items(
        &self,
        key_items: &[SemanticIndexKeyItem],
        prefix_len: usize,
    ) -> GroupedIndexOrderMatch {
        let index_len = key_items.len();
        let matches =
            |index: usize, term: &str| key_items[index].as_ref().matches_canonical_text(term);
        // Trailing index terms preserve grouped-key contiguity; unlike scalar
        // ordering, grouped matching only requires the leading sequence.
        if prefix_len > 0 && order_terms_match_at(&self.terms, index_len, prefix_len, &matches) {
            return GroupedIndexOrderMatch::Suffix;
        }
        if order_terms_match_at(&self.terms, index_len, 0, &matches) {
            return GroupedIndexOrderMatch::Full;
        }

        GroupedIndexOrderMatch::None
    }
}

// Check the complete range before invoking either canonical-label comparator.
// Offsets beyond the key list never index it, including usize::MAX inputs.
fn order_terms_match_at(
    terms: &[String],
    index_len: usize,
    offset: usize,
    matches: &impl Fn(usize, &str) -> bool,
) -> bool {
    offset <= index_len
        && terms.len() <= index_len - offset
        && terms
            .iter()
            .enumerate()
            .all(|(index, term)| matches(offset + index, term))
}

/// Return whether accepted field-path index order terms satisfy one grouped
/// ORDER BY contract after the equality-bound prefix.
#[must_use]
pub(in crate::db) fn grouped_index_key_items_satisfied(
    order_contract: &GroupedIndexOrderContract,
    key_items: &[SemanticIndexKeyItem],
    prefix_len: usize,
) -> bool {
    !matches!(
        order_contract.classify_index_key_items(key_items, prefix_len),
        GroupedIndexOrderMatch::None
    )
}

impl OrderSpec {
    /// Return ordering direction when `ORDER BY` is exactly the ordered
    /// primary-key field list and every term has the same direction.
    #[must_use]
    pub(in crate::db) fn primary_key_only_direction_fields(
        &self,
        primary_key_names: &[String],
    ) -> Option<OrderDirection> {
        if primary_key_names.is_empty() || self.fields.len() != primary_key_names.len() {
            return None;
        }

        let direction = self.fields.first()?.direction();
        self.fields
            .iter()
            .zip(primary_key_names.iter())
            .all(|(term, primary_key_name)| {
                term.direct_field() == Some(primary_key_name.as_str())
                    && term.direction() == direction
            })
            .then_some(direction)
    }

    /// Return the normalized deterministic `..., primary_key_fields` order
    /// contract, if one exists for this ORDER BY shape.
    #[must_use]
    pub(in crate::db) fn deterministic_secondary_order_contract_fields(
        &self,
        primary_key_names: Rc<[String]>,
    ) -> Option<DeterministicSecondaryOrderContract> {
        DeterministicSecondaryOrderContract::from_order_spec_fields(self, primary_key_names)
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

fn has_exact_ordered_primary_key_tie_break_fields(
    fields: &[crate::db::query::plan::OrderTerm],
    primary_key_names: &[String],
) -> bool {
    if primary_key_names.is_empty() || fields.len() < primary_key_names.len() {
        return false;
    }

    let split = fields.len() - primary_key_names.len();
    let (prefix, suffix) = fields.split_at(split);
    if !suffix
        .iter()
        .zip(primary_key_names.iter())
        .all(|(term, primary_key_name)| term.direct_field() == Some(primary_key_name.as_str()))
    {
        return false;
    }

    !prefix.iter().any(|term| {
        term.direct_field()
            .is_some_and(|field| primary_key_names.iter().any(|name| name == field))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DeterministicSecondaryOrderContract, GroupedIndexOrderContract, GroupedIndexOrderMatch,
    };
    use crate::db::access::AccessPathKind::{
        IndexBranchSet, IndexMultiLookup, IndexPrefix, IndexRange,
    };
    use crate::db::query::plan::{OrderDirection, OrderSpec, OrderTerm, expr::Expr};
    use crate::retained::RetainedBytes;
    use crate::value::Value;
    use std::rc::Rc;

    #[test]
    fn key_item_classification_preserves_scalar_and_grouped_label_semantics() {
        use super::DeterministicSecondaryIndexOrderMatch as ScalarMatch;
        use crate::db::{
            access::SemanticIndexKeyItem, index::SemanticIndexExpression,
            schema::PersistedIndexExpressionOp,
        };

        for op in [
            PersistedIndexExpressionOp::Lower,
            PersistedIndexExpressionOp::Upper,
            PersistedIndexExpressionOp::Trim,
            PersistedIndexExpressionOp::LowerTrim,
            PersistedIndexExpressionOp::Date,
            PersistedIndexExpressionOp::Year,
            PersistedIndexExpressionOp::Month,
            PersistedIndexExpressionOp::Day,
        ] {
            let expression = SemanticIndexExpression::new(op, "账户".to_string());
            let label = expression.canonical_order_text();
            let field = |name: &str| SemanticIndexKeyItem::Field(name.to_string());
            for items in [
                vec![],
                vec![SemanticIndexKeyItem::Expression(expression.clone())],
                vec![
                    field("prefix"),
                    SemanticIndexKeyItem::Expression(expression.clone()),
                    field("tenant"),
                    field("id"),
                ],
                vec![
                    SemanticIndexKeyItem::Expression(expression.clone()),
                    field("tenant"),
                    field("id"),
                ],
                vec![field(&label), field("tenant"), field("id")],
                vec![
                    SemanticIndexKeyItem::Expression(expression.clone()),
                    field("id"),
                    field("tenant"),
                ],
            ] {
                let rendered = super::index_key_item_order_terms(&items);
                for keys in [
                    vec!["id".to_string()],
                    vec!["tenant".to_string(), "id".to_string()],
                ] {
                    // This direct sequence oracle pins suffix stripping and
                    // precedence independently of the shared callback matcher.
                    let scalar_index = rendered.strip_suffix(keys.as_slice()).unwrap_or(&rendered);
                    for terms in [
                        vec![],
                        vec![label.clone()],
                        vec!["prefix".to_string(), label.clone()],
                        vec!["missing".to_string()],
                    ] {
                        let scalar = DeterministicSecondaryOrderContract {
                            non_primary_key_terms: terms.clone(),
                            primary_key_terms: Rc::from(keys.clone()),
                            direction: OrderDirection::Asc,
                        };
                        let grouped = GroupedIndexOrderContract {
                            terms: terms.clone(),
                            direction: OrderDirection::Asc,
                        };
                        for prefix in (0..=items.len() + 1).chain([usize::MAX]) {
                            let scalar_expected = if scalar_index
                                .get(prefix..)
                                .is_some_and(|suffix| suffix == terms)
                            {
                                ScalarMatch::Suffix
                            } else if scalar_index == terms {
                                ScalarMatch::Full
                            } else {
                                ScalarMatch::None
                            };
                            let grouped_expected = if prefix > 0
                                && rendered
                                    .get(prefix..)
                                    .is_some_and(|suffix| suffix.starts_with(&terms))
                            {
                                GroupedIndexOrderMatch::Suffix
                            } else if rendered.starts_with(&terms) {
                                GroupedIndexOrderMatch::Full
                            } else {
                                GroupedIndexOrderMatch::None
                            };
                            assert_eq!(
                                scalar.classify_index_key_items(&items, prefix),
                                scalar_expected
                            );
                            assert_eq!(
                                scalar.classify_index_match(&rendered, prefix),
                                scalar_expected
                            );
                            assert_eq!(
                                grouped.classify_index_key_items(&items, prefix),
                                grouped_expected
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn borrowed_primary_key_names_preserve_exact_tuple_and_suffix_admission() {
        let names: Rc<[String]> = Rc::from(["tenant".to_string(), "id".to_string()]);
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            for (fields, exact, suffix) in [
                (vec!["tenant", "id"], true, true),
                (vec!["created", "tenant", "id"], false, true),
                (vec!["id"], false, false),
                (vec!["id", "tenant"], false, false),
                (vec!["tenant", "tenant", "id"], false, false),
                (vec!["id", "created", "tenant", "id"], false, false),
                (vec![], false, false),
            ] {
                let order = OrderSpec {
                    fields: fields
                        .iter()
                        .map(|field| OrderTerm::field(*field, direction))
                        .collect(),
                };
                assert_eq!(
                    order.primary_key_only_direction_fields(&names),
                    exact.then_some(direction)
                );
                let contract =
                    order.deterministic_secondary_order_contract_fields(Rc::clone(&names));
                assert_eq!(contract.is_some(), suffix);
                if let Some(contract) = contract {
                    assert_eq!(contract.primary_key_terms, names);
                    assert!(Rc::ptr_eq(&contract.primary_key_terms, &names));
                    assert_eq!(
                        contract.non_primary_key_terms(),
                        &fields[..fields.len() - names.len()]
                    );
                    assert_eq!(contract.direction(), direction);
                }
            }
        }
    }

    #[test]
    fn borrowed_primary_key_order_rejects_empty_keys_mixed_directions_and_expressions() {
        let names: Rc<[String]> = Rc::from(["tenant".to_string(), "id".to_string()]);
        let mixed = OrderSpec {
            fields: vec![
                OrderTerm::field("tenant", OrderDirection::Asc),
                OrderTerm::field("id", OrderDirection::Desc),
            ],
        };
        let expression = OrderSpec {
            fields: vec![
                OrderTerm::field("tenant", OrderDirection::Asc),
                OrderTerm::new(Expr::Literal(Value::Nat64(1)), OrderDirection::Asc),
            ],
        };
        for order in [mixed, expression] {
            for names in [Rc::clone(&names), Rc::from([])] {
                assert_eq!(order.primary_key_only_direction_fields(&names), None);
                assert!(
                    order
                        .deterministic_secondary_order_contract_fields(names)
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn secondary_order_clones_share_detached_key_names_and_count_retention() {
        // Spare string capacity must remain counted even though contracts share
        // the payload. Each resident conservatively accounts for its full share.
        let mut tenant = String::with_capacity(64);
        tenant.push_str("账户");
        let names: Rc<[String]> = Rc::from([tenant, "id".to_string()]);
        let expected = size_of::<DeterministicSecondaryOrderContract>()
            + 2 * size_of::<usize>()
            + size_of_val(names.as_ref())
            + names.iter().map(String::capacity).sum::<usize>();
        let order = OrderSpec {
            fields: names
                .iter()
                .map(|name| OrderTerm::field(name, OrderDirection::Asc))
                .collect(),
        };
        let contract = order
            .deterministic_secondary_order_contract_fields(Rc::clone(&names))
            .unwrap();
        let cloned = contract.clone();
        assert!(Rc::ptr_eq(&contract.primary_key_terms, &names));
        assert!(Rc::ptr_eq(&cloned.primary_key_terms, &names));
        drop(names);
        drop(order);
        for resident in [contract, cloned] {
            assert_eq!(
                resident.classify_index_match(&["账户", "id"], 0),
                super::DeterministicSecondaryIndexOrderMatch::Suffix
            );
            assert_eq!(RetainedBytes::measure(&resident, expected), Some(expected));
            assert_eq!(RetainedBytes::measure(&resident, expected - 1), None);
        }
    }

    fn grouped_contract(terms: &[&str]) -> GroupedIndexOrderContract {
        GroupedIndexOrderContract {
            terms: terms.iter().map(ToString::to_string).collect(),
            direction: OrderDirection::Asc,
        }
    }

    #[test]
    fn grouped_order_accepts_trailing_index_tie_break_terms() {
        let contract = grouped_contract(&["group_key"]);
        let index =
            ["group_key", "id"].map(|field| super::SemanticIndexKeyItem::Field(field.into()));

        assert_eq!(
            contract.classify_index_key_items(&index, 0),
            GroupedIndexOrderMatch::Full
        );
    }

    #[test]
    fn grouped_order_accepts_trailing_terms_after_equality_prefix() {
        let contract = grouped_contract(&["group_key"]);
        let index = ["tenant_id", "group_key", "id"]
            .map(|field| super::SemanticIndexKeyItem::Field(field.into()));

        assert_eq!(
            contract.classify_index_key_items(&index, 1),
            GroupedIndexOrderMatch::Suffix
        );
    }

    #[test]
    fn grouped_order_rejects_a_gap_before_the_group_key() {
        let contract = grouped_contract(&["group_key"]);
        let index = ["tenant_id", "created_at", "group_key", "id"]
            .map(|field| super::SemanticIndexKeyItem::Field(field.into()));

        assert_eq!(
            contract.classify_index_key_items(&index, 1),
            GroupedIndexOrderMatch::None
        );
    }

    #[test]
    fn variable_prefix_access_kinds_require_a_full_secondary_order_match() {
        for access_kind in [IndexMultiLookup, IndexBranchSet] {
            assert!(
                DeterministicSecondaryOrderContract::access_kind_requires_full_index_order(
                    access_kind,
                )
            );
        }
        for access_kind in [IndexPrefix, IndexRange] {
            assert!(
                !DeterministicSecondaryOrderContract::access_kind_requires_full_index_order(
                    access_kind,
                )
            );
        }
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(DeterministicSecondaryOrderContract {
Self{non_primary_key_terms,primary_key_terms,direction} => [non_primary_key_terms,primary_key_terms,direction],
});
crate::retained::retained_fields!(ExecutionOrderContract {
Self{ordering,direction,supports_cursor} => [ordering,direction,supports_cursor],
});
crate::retained::retained_fields!(ExecutionOrdering {
Self::PrimaryKey => [],
Self::Explicit(field_0) => [field_0],
Self::Grouped(field_0) => [field_0],
});
