//! Module: query::plan::cardinality_tiebreak
//! Responsibility: bounded exact-cardinality tie-break contracts retained by one plan.
//! Does not own: cardinality storage, cache refresh policy, or cursor encoding.
//! Boundary: existing tied access candidates -> advisory selection evidence and route identity.

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        access::{AccessPlan, SemanticIndexAccessContract},
        index::IndexId,
        registry::ExactPrefixCardinalityLifecycleStamp,
    },
    types::EntityTag,
    value::Value,
};

/// Existing single-index access families eligible for exact-cardinality tie-breaking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CardinalityTiebreakFamily {
    Prefix,
    MultiLookup,
    BranchSet,
}

impl CardinalityTiebreakFamily {
    #[must_use]
    pub(in crate::db) const fn wire_tag(self) -> u8 {
        match self {
            Self::Prefix => 1,
            Self::MultiLookup => 2,
            Self::BranchSet => 3,
        }
    }

    pub(in crate::db) const fn from_wire_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Prefix),
            2 => Some(Self::MultiLookup),
            3 => Some(Self::BranchSet),
            _ => None,
        }
    }
}

/// Authenticated identity of one accepted cardinality-ranked route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityTiebreakRoutePin {
    index_id: IndexId,
    family: CardinalityTiebreakFamily,
    consumed_prefix_arity: u8,
}

impl CardinalityTiebreakRoutePin {
    #[must_use]
    pub(in crate::db) fn new(
        index_id: IndexId,
        family: CardinalityTiebreakFamily,
        consumed_prefix_arity: usize,
    ) -> Option<Self> {
        if consumed_prefix_arity == 0 || consumed_prefix_arity > MAX_INDEX_FIELDS {
            return None;
        }

        Some(Self {
            index_id,
            family,
            consumed_prefix_arity: u8::try_from(consumed_prefix_arity).ok()?,
        })
    }

    #[must_use]
    pub(in crate::db) const fn index_id(self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db) const fn family(self) -> CardinalityTiebreakFamily {
        self.family
    }

    #[must_use]
    pub(in crate::db) const fn consumed_prefix_arity(self) -> u8 {
        self.consumed_prefix_arity
    }
}

/// One existing planner candidate that survived every structural and residual rank.
#[derive(Clone, Debug)]
pub(in crate::db) struct CardinalityTiebreakCandidate {
    access: AccessPlan<Value>,
    index: SemanticIndexAccessContract,
    family: CardinalityTiebreakFamily,
    consumed_prefix_arity: usize,
}

impl CardinalityTiebreakCandidate {
    #[must_use]
    pub(in crate::db::query) const fn new(
        access: AccessPlan<Value>,
        index: SemanticIndexAccessContract,
        family: CardinalityTiebreakFamily,
        consumed_prefix_arity: usize,
    ) -> Self {
        Self {
            access,
            index,
            family,
            consumed_prefix_arity,
        }
    }

    #[must_use]
    pub(in crate::db) const fn access(&self) -> &AccessPlan<Value> {
        &self.access
    }

    #[must_use]
    pub(in crate::db) fn into_access(self) -> AccessPlan<Value> {
        self.access
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &SemanticIndexAccessContract {
        &self.index
    }

    #[must_use]
    pub(in crate::db) fn route_pin(
        &self,
        entity_tag: EntityTag,
    ) -> Option<CardinalityTiebreakRoutePin> {
        CardinalityTiebreakRoutePin::new(
            IndexId::new_with_generation(
                entity_tag,
                self.index.ordinal(),
                self.index.physical_generation(),
            ),
            self.family,
            self.consumed_prefix_arity,
        )
    }
}

/// Privacy-safe exact entry count retained for one tied semantic candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CardinalityTiebreakCandidateEvidence {
    index_name: String,
    exact_prefix_entries: u64,
}

impl CardinalityTiebreakCandidateEvidence {
    #[must_use]
    pub(in crate::db) const fn new(index_name: String, exact_prefix_entries: u64) -> Self {
        Self {
            index_name,
            exact_prefix_entries,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn exact_prefix_entries(&self) -> u64 {
        self.exact_prefix_entries
    }
}

/// Exact selection-time evidence frozen into one prepared plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExactCardinalityTiebreakEvidence {
    route_pin: CardinalityTiebreakRoutePin,
    candidates: Vec<CardinalityTiebreakCandidateEvidence>,
}

impl ExactCardinalityTiebreakEvidence {
    #[must_use]
    pub(in crate::db) const fn new(
        route_pin: CardinalityTiebreakRoutePin,
        candidates: Vec<CardinalityTiebreakCandidateEvidence>,
    ) -> Self {
        Self {
            route_pin,
            candidates,
        }
    }

    #[must_use]
    pub(in crate::db) const fn route_pin(&self) -> CardinalityTiebreakRoutePin {
        self.route_pin
    }

    #[must_use]
    pub(in crate::db) const fn candidates(&self) -> &[CardinalityTiebreakCandidateEvidence] {
        self.candidates.as_slice()
    }
}

/// Cache-retained result of the optional exact-cardinality decision.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) enum CardinalityTiebreakState {
    #[default]
    NotApplicable,
    ExactAtSelection(ExactCardinalityTiebreakEvidence),
    PinnedContinuation(CardinalityTiebreakRoutePin),
    Unavailable {
        lifecycle_stamp: ExactPrefixCardinalityLifecycleStamp,
        route_pin: CardinalityTiebreakRoutePin,
    },
    PolicyFallback(CardinalityTiebreakRoutePin),
}

impl CardinalityTiebreakState {
    #[must_use]
    pub(in crate::db) const fn route_pin(&self) -> Option<CardinalityTiebreakRoutePin> {
        match self {
            Self::ExactAtSelection(evidence) => Some(evidence.route_pin()),
            Self::PinnedContinuation(route_pin) | Self::PolicyFallback(route_pin) => {
                Some(*route_pin)
            }
            Self::Unavailable { route_pin, .. } => Some(*route_pin),
            Self::NotApplicable => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn unavailable_stamp(
        &self,
    ) -> Option<ExactPrefixCardinalityLifecycleStamp> {
        match self {
            Self::Unavailable {
                lifecycle_stamp, ..
            } => Some(*lifecycle_stamp),
            Self::NotApplicable
            | Self::ExactAtSelection(_)
            | Self::PinnedContinuation(_)
            | Self::PolicyFallback(_) => None,
        }
    }
}
