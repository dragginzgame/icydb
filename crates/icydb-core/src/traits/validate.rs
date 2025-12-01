use icydb_error::ErrorTree;
use std::collections::{HashMap, HashSet};

///
/// Validate
///

pub trait Validate: ValidateAuto + ValidateCustom {}

impl<T> Validate for T where T: ValidateAuto + ValidateCustom {}

///
/// ValidateContext
///
/// Context that can be provided during validation.
///
/// NOTE: ValidateContext is reserved for future context-aware sanitization.
/// The *_with() methods are currently thin wrappers that delegate to the
/// stateless versions. In the future, we may pass runtime data (e.g. now, is_new,
/// actor) here so validators can behave contextually without changing the trait shape.
///

#[derive(Clone, Debug, Default)]
pub struct ValidateContext;

///
/// ValidateAuto
///
/// derived code that is used to generate the validation rules for a type and
/// its children, via schema validation rules
///
/// this shouldn't be used with primitive types, it's only really for validation
/// rules put in by macros
///

pub trait ValidateAuto {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        Ok(())
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        Ok(())
    }

    fn validate_self_with(&self, _ctx: &ValidateContext) -> Result<(), ErrorTree> {
        self.validate_self()
    }

    fn validate_children_with(&self, _ctx: &ValidateContext) -> Result<(), ErrorTree> {
        self.validate_children()
    }
}

impl<T: ValidateAuto> ValidateAuto for Box<T> {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        (**self).validate_self()
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        (**self).validate_children()
    }
}

impl<T: ValidateAuto> ValidateAuto for Option<T> {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        self.as_ref().map_or(Ok(()), ValidateAuto::validate_self)
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        self.as_ref()
            .map_or(Ok(()), ValidateAuto::validate_children)
    }
}

impl<T: ValidateAuto> ValidateAuto for Vec<T> {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateAuto::validate_self))
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateAuto::validate_children))
    }
}

impl<T: ValidateAuto, S> ValidateAuto for HashSet<T, S> {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateAuto::validate_self))
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateAuto::validate_children))
    }
}

impl<K: ValidateAuto, V: ValidateAuto, S> ValidateAuto for HashMap<K, V, S> {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(
            self.iter()
                .flat_map(|(k, v)| [k.validate_self(), v.validate_self()]),
        )
    }

    fn validate_children(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(
            self.iter()
                .flat_map(|(k, v)| [k.validate_children(), v.validate_children()]),
        )
    }
}

impl_primitive!(ValidateAuto);

///
/// ValidateCustom
///
/// custom validation behaviour that can be added to any type
///

pub trait ValidateCustom {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        Ok(())
    }

    fn validate_custom_with(&self, _ctx: &ValidateContext) -> Result<(), ErrorTree> {
        self.validate_custom()
    }
}

impl<T: ValidateCustom> ValidateCustom for Box<T> {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        (**self).validate_custom()
    }
}

impl<T: ValidateCustom> ValidateCustom for Option<T> {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        self.as_ref()
            .map_or(Ok(()), ValidateCustom::validate_custom)
    }
}

impl<T: ValidateCustom> ValidateCustom for Vec<T> {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateCustom::validate_custom))
    }
}

impl<T: ValidateCustom, S> ValidateCustom for HashSet<T, S> {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(self.iter().map(ValidateCustom::validate_custom))
    }
}

impl<K: ValidateCustom, V: ValidateCustom, S> ValidateCustom for HashMap<K, V, S> {
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        ErrorTree::collect(
            self.iter()
                .flat_map(|(k, v)| [k.validate_custom(), v.validate_custom()]),
        )
    }
}

impl_primitive!(ValidateCustom);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    /// A dummy type that always fails validation
    #[derive(Debug, Eq, Hash, PartialEq)]
    struct Bad;

    impl ValidateAuto for Bad {
        fn validate_self(&self) -> Result<(), ErrorTree> {
            Err("bad self".into())
        }
        fn validate_children(&self) -> Result<(), ErrorTree> {
            Err("bad children".into())
        }
    }

    impl ValidateCustom for Bad {
        fn validate_custom(&self) -> Result<(), ErrorTree> {
            Err("bad custom".into())
        }
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)]
    fn hashmap_collects_key_and_value_errors() {
        let mut map = HashMap::new();
        map.insert(Bad, Bad);

        // Run self-validation
        let result = map.validate_self();

        assert!(result.is_err(), "expected error from validation");
        let errs = result.unwrap_err();

        // Flatten should contain both key and value errors
        let flat = errs.flatten_ref();

        // At least 2 distinct errors should be present
        assert!(
            flat.iter().any(|(_, msg)| msg.contains("bad self")),
            "missing key/value self errors: {flat:?}",
        );
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)]
    fn hashmap_collects_custom_errors() {
        let mut map = HashMap::new();
        map.insert(Bad, Bad);

        let result = map.validate_custom();

        assert!(result.is_err(), "expected error from custom validation");
        let errs = result.unwrap_err();

        let flat = errs.flatten_ref();
        assert!(
            flat.iter().any(|(_, msg)| msg.contains("bad custom")),
            "missing key/value custom errors: {flat:?}",
        );
    }
}
