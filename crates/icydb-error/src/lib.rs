use candid::CandidType;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

///
/// ErrorTree
///
#[derive(CandidType, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorTree {
    /// errors at the current level
    pub messages: Vec<String>,

    /// child errors indexed by field/key
    pub children: HashMap<String, ErrorTree>,
}

impl ErrorTree {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn collect<I>(iter: I) -> Result<(), Self>
    where
        I: IntoIterator<Item = Result<(), Self>>,
    {
        let mut errs = Self::new();
        for res in iter {
            if let Err(e) = res {
                errs.merge(e);
            }
        }

        errs.result()
    }

    // add
    // add an error message to the current level
    pub fn add<M: ToString>(&mut self, message: M) {
        self.messages.push(message.to_string());
    }

    // add_result
    pub fn add_result<M: ToString>(&mut self, error: Result<(), M>) {
        if let Err(e) = error {
            self.messages.push(e.to_string());
        }
    }

    // addf: format and add an error message
    pub fn addf(&mut self, args: fmt::Arguments) {
        self.messages.push(format!("{args}"));
    }

    // add_for
    // add an error message under a specific key
    pub fn add_for<K: ToString, M: ToString>(&mut self, key: K, message: M) {
        self.children
            .entry(key.to_string())
            .or_default()
            .add(message);
    }

    /// Merge another ErrorTree structure into this one.
    /// Child errors are merged recursively.
    pub fn merge(&mut self, other: Self) {
        self.messages.extend(other.messages);
        for (key, child_errors) in other.children {
            self.children.entry(key).or_default().merge(child_errors);
        }
    }

    /// Check if there are any errors.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.children.is_empty()
    }

    // flatten the error hierarchy without consuming self
    #[must_use]
    pub fn flatten_ref(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        self.flatten_helper_ref(String::new(), &mut result);
        result
    }

    // flatten_helper_ref
    fn flatten_helper_ref(&self, prefix: String, result: &mut Vec<(String, String)>) {
        // Add messages at the current level.
        for msg in &self.messages {
            result.push((prefix.clone(), msg.clone()));
        }
        // Process child errors recursively.
        for (key, child) in &self.children {
            let new_prefix = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            child.flatten_helper_ref(new_prefix, result);
        }
    }

    /// Consume self and return Ok(()) if there are no errors,
    /// or Err(self) otherwise.
    pub fn result(self) -> Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

#[macro_export]
macro_rules! err {
    ($errs:expr, $($arg:tt)*) => {{
        $errs.addf(format_args!($($arg)*));
    }};
}

impl fmt::Display for ErrorTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (key, msg) in self.flatten_ref() {
            if key.is_empty() {
                writeln!(f, "{msg}")?;
            } else {
                writeln!(f, "{key}: {msg}")?;
            }
        }

        Ok(())
    }
}

impl From<&str> for ErrorTree {
    fn from(err: &str) -> Self {
        let mut tree = Self::new();
        tree.add(err.to_string());

        tree
    }
}

impl From<String> for ErrorTree {
    fn from(s: String) -> Self {
        let mut tree = Self::new();
        tree.add(s);

        tree
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_errors() {
        let errs = ErrorTree::new();
        assert!(errs.is_empty());
        assert_eq!(errs.result(), Ok(()));
    }

    #[test]
    fn test_add_and_merge() {
        let mut errs = ErrorTree::new();
        errs.add("top-level error");

        let mut child_errs = ErrorTree::new();
        child_errs.add("child error 1");
        child_errs.add("child error 2");
        errs.add_for("field", "field error");
        errs.children
            .entry("nested".to_string())
            .or_default()
            .merge(child_errs);

        // Check hierarchical structure.
        assert_eq!(errs.messages.len(), 1);
        assert!(errs.children.contains_key("field") || errs.children.contains_key("nested"));

        // Flatten and check that errors include keys.
        let flat = errs.flatten_ref();
        assert_eq!(flat.len(), 4);
    }
}
