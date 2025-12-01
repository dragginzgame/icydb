use crate::{
    Error, ThisError,
    traits::Visitable,
    visitor::{Event, PathSegment, Visitor, VisitorError, perform_visit},
};
use icydb_error::ErrorTree;
use std::fmt::Write;

///
/// ValidateError
///

#[derive(Debug, ThisError)]
pub enum ValidateError {
    #[error("validation failed: {0}")]
    ValidationFailed(ErrorTree),
}

impl From<ValidateError> for Error {
    fn from(err: ValidateError) -> Self {
        VisitorError::from(err).into()
    }
}

// validate
pub fn validate(node: &dyn Visitable) -> Result<(), ValidateError> {
    let mut visitor = ValidateVisitor::new();
    perform_visit(&mut visitor, node, PathSegment::Empty);

    visitor
        .errors
        .result()
        .map_err(ValidateError::ValidationFailed)?;

    Ok(())
}

///
/// ValidateVisitor
///

#[derive(Debug, Default)]
pub struct ValidateVisitor {
    pub errors: ErrorTree,
    pub path: Vec<PathSegment>,
}

impl ValidateVisitor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn current_route(&self) -> String {
        let mut out = String::new();
        let mut first = true;

        for seg in &self.path {
            match seg {
                PathSegment::Field(s) if !s.is_empty() => {
                    if !first {
                        out.push('.');
                    }
                    out.push_str(s);
                    first = false;
                }
                PathSegment::Index(i) => {
                    // indices shown as [0], [1], ...
                    let _ = write!(out, "[{i}]");
                    first = false;
                }
                _ => {}
            }
        }

        out
    }
}

impl Visitor for ValidateVisitor {
    #[inline]
    fn visit(&mut self, node: &dyn Visitable, event: Event) {
        match event {
            Event::Enter => {
                let mut errs = ErrorTree::new();

                // combine all validation types
                // better to do it here and not in the trait
                if let Err(e) = node.validate_self() {
                    errs.merge(e);
                }
                if let Err(e) = node.validate_children() {
                    errs.merge(e);
                }
                if let Err(e) = node.validate_custom() {
                    errs.merge(e);
                }

                // check for errs
                if !errs.is_empty() {
                    if self.path.is_empty() {
                        // At the current level, merge directly.
                        self.errors.merge(errs);
                    } else {
                        // Add to a child entry under the computed route.
                        let route = self.current_route();
                        self.errors.children.entry(route).or_default().merge(errs);
                    }
                }
            }
            Event::Exit => {}
        }
    }

    #[inline]
    fn push(&mut self, seg: PathSegment) {
        if !matches!(seg, PathSegment::Empty) {
            self.path.push(seg);
        }
    }

    #[inline]
    fn pop(&mut self) {
        self.path.pop();
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        traits::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
        visitor::{perform_visit, validate},
    };

    const ERR_MSG: &str = "leaf error";

    // A simple leaf type that can emit an error based on a flag.
    #[derive(Clone, Debug, Default)]
    struct Leaf(bool);

    impl SanitizeAuto for Leaf {}
    impl SanitizeCustom for Leaf {}
    impl Visitable for Leaf {}
    impl ValidateAuto for Leaf {}
    impl ValidateCustom for Leaf {
        fn validate_custom(&self) -> Result<(), ErrorTree> {
            if self.0 {
                Err(ErrorTree::from(ERR_MSG))
            } else {
                Ok(())
            }
        }
    }

    // Helper: get flattened errors from validate()
    fn flatten_errs(node: &dyn Visitable) -> Vec<(String, String)> {
        match validate(node) {
            Ok(()) => Vec::new(),
            Err(crate::visitor::ValidateError::ValidationFailed(tree)) => tree.flatten_ref(),
        }
    }

    // Root-level error should attach to the root (empty route)
    #[test]
    fn root_level_error_is_at_root() {
        let leaf = Leaf(true);
        let flat = flatten_errs(&leaf);

        assert!(flat.iter().any(|(k, v)| k.is_empty() && v == ERR_MSG));
    }

    // Container with a Vec field should index items: "field.1"
    #[test]
    fn record_field_vec_item_path_is_indexed() {
        #[derive(Debug, Default)]
        struct Container {
            nums: Vec<Leaf>,
        }

        impl SanitizeAuto for Container {}
        impl SanitizeCustom for Container {}
        impl ValidateAuto for Container {}
        impl ValidateCustom for Container {}
        impl Visitable for Container {
            fn drive(&self, visitor: &mut dyn Visitor) {
                // Record field key then Vec indices
                perform_visit(visitor, &self.nums, "nums");
            }
        }

        let node = Container {
            nums: vec![Leaf(false), Leaf(true), Leaf(false)],
        };

        let flat = flatten_errs(&node);
        assert!(flat.iter().any(|(k, v)| k == "nums[1]" && v == ERR_MSG));
    }

    // Nested record, tuple-like, and map-like structures should produce dotted keys.
    #[test]
    fn nested_record_tuple_map_paths_are_dotted() {
        // Inner record with a single leaf field
        #[derive(Debug, Default)]
        struct Inner {
            leaf: Leaf,
        }

        impl SanitizeAuto for Inner {}
        impl SanitizeCustom for Inner {}
        impl ValidateAuto for Inner {}
        impl ValidateCustom for Inner {}
        impl Visitable for Inner {
            fn drive(&self, visitor: &mut dyn Visitor) {
                perform_visit(visitor, &self.leaf, "leaf");
            }
        }

        // Tuple-like struct with two leaves; use indices "0", "1"
        #[derive(Debug, Default)]
        struct Tup2(Leaf, Leaf);

        impl SanitizeAuto for Tup2 {}
        impl SanitizeCustom for Tup2 {}
        impl ValidateAuto for Tup2 {}
        impl ValidateCustom for Tup2 {}
        impl Visitable for Tup2 {
            fn drive(&self, visitor: &mut dyn Visitor) {
                perform_visit(visitor, &self.0, 0);
                perform_visit(visitor, &self.1, 1);
            }
        }

        // Simple map-like wrapper iterating key/value pairs
        #[derive(Debug, Default)]
        struct MyMap(Vec<(String, Leaf)>);

        impl SanitizeAuto for MyMap {}
        impl SanitizeCustom for MyMap {}
        impl ValidateAuto for MyMap {}
        impl ValidateCustom for MyMap {}
        impl Visitable for MyMap {
            fn drive(&self, visitor: &mut dyn Visitor) {
                for (_k, v) in &self.0 {
                    // Align with macro-generated map visitor: push "value"
                    perform_visit(visitor, v, "value");
                }
            }
        }

        #[derive(Debug, Default)]
        struct Outer {
            rec: Inner,
            tup: Tup2,
            map: MyMap,
        }

        impl SanitizeAuto for Outer {}
        impl SanitizeCustom for Outer {}
        impl ValidateAuto for Outer {}
        impl ValidateCustom for Outer {}
        impl Visitable for Outer {
            fn drive(&self, visitor: &mut dyn Visitor) {
                perform_visit(visitor, &self.rec, "rec");
                perform_visit(visitor, &self.tup, "tup");
                perform_visit(visitor, &self.map, "map");
            }
        }

        let node = Outer {
            rec: Inner { leaf: Leaf(true) },
            tup: Tup2(Leaf(false), Leaf(true)),
            map: MyMap(vec![("k".to_string(), Leaf(true))]),
        };

        let flat = flatten_errs(&node);

        // Expect errors at specific dotted paths
        assert!(flat.iter().any(|(k, v)| k == "rec.leaf" && v == ERR_MSG));
        assert!(flat.iter().any(|(k, v)| k == "tup[1]" && v == ERR_MSG));
        assert!(flat.iter().any(|(k, v)| k == "map.value" && v == ERR_MSG));
    }
}
