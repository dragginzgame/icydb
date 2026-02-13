use thiserror::Error as ThisError;

///
/// MergePatchError
///
/// Structured failures for user-driven patch application.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum MergePatchError {
    #[error("invalid patch shape: expected {expected}, found {actual}")]
    InvalidShape {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("invalid patch cardinality: expected {expected}, found {actual}")]
    CardinalityViolation { expected: usize, actual: usize },

    #[error("patch merge failed at {path}: {source}")]
    Context {
        path: String,
        #[source]
        source: Box<Self>,
    },
}

impl MergePatchError {
    /// Prepend a field segment to the merge error path.
    #[must_use]
    pub fn with_field(self, field: impl AsRef<str>) -> Self {
        self.with_path_segment(field.as_ref())
    }

    /// Prepend an index segment to the merge error path.
    #[must_use]
    pub fn with_index(self, index: usize) -> Self {
        self.with_path_segment(format!("[{index}]"))
    }

    /// Return the full contextual path, if available.
    #[must_use]
    pub const fn path(&self) -> Option<&str> {
        match self {
            Self::Context { path, .. } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Return the innermost, non-context merge error variant.
    #[must_use]
    pub fn leaf(&self) -> &Self {
        match self {
            Self::Context { source, .. } => source.leaf(),
            _ => self,
        }
    }

    #[must_use]
    fn with_path_segment(self, segment: impl Into<String>) -> Self {
        let segment = segment.into();
        match self {
            Self::Context { path, source } => Self::Context {
                path: Self::join_segments(segment.as_str(), path.as_str()),
                source,
            },
            source => Self::Context {
                path: segment,
                source: Box::new(source),
            },
        }
    }

    #[must_use]
    fn join_segments(prefix: &str, suffix: &str) -> String {
        if suffix.starts_with('[') {
            format!("{prefix}{suffix}")
        } else {
            format!("{prefix}.{suffix}")
        }
    }
}
