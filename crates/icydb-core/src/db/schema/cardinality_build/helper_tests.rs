//! Test-only constructors and adapters for the parent module.

use super::*;

impl CardinalityBuildAuthority {
    pub(super) fn from_parts(
        source: CardinalitySourceIdentity,
        accepted_entities: BTreeSet<EntityTag>,
        accepted_indexes: BTreeMap<IndexId, usize>,
    ) -> Self {
        let accepted_index_set =
            CardinalityAcceptedIndexSetIdentity::derive(accepted_indexes.keys().copied())
                .expect("test accepted index set should derive");
        Self {
            source,
            domain: Some(Rc::new(CardinalityAcceptedDomain {
                entities: accepted_entities,
                indexes: accepted_indexes,
                index_set: accepted_index_set,
            })),
        }
    }
}
