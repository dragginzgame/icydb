//! Test-only constructors and adapters for the parent module.

use super::*;

impl ExplainExecutionNodeDescriptor {
    /// Return whether this descriptor tree contains the requested node type.
    #[must_use]
    pub(in crate::db) fn contains_type(&self, target: ExplainExecutionNodeType) -> bool {
        let mut found = false;
        self.for_each_preorder(&mut |node| {
            if node.node_type() == target {
                found = true;
            }
        });

        found
    }
}
