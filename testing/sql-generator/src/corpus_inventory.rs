//! Module: sql_generator::corpus_inventory
//! Responsibility: strict checked-in SQL regression corpus filesystem inventory.
//! Does not own: corpus encoding, failure review, semantic execution, or mismatch verdicts.
//! Boundary: admits only bounded canonical entries whose filename matches their reviewed identity.

use crate::{REGRESSION_CORPUS_MAX_ENTRY_BYTES, RegressionCorpusEntry, SqlGeneratorError};

use std::{
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

/// Load the complete checked-in regression corpus in stable identity order.
///
/// # Errors
///
/// Returns a typed inventory error when the corpus directory cannot be read or
/// contains an unsupported, oversized, stale, non-canonical, or misnamed entry.
pub fn checked_in_regression_corpus()
-> Result<Vec<RegressionCorpusEntry>, RegressionCorpusInventoryError> {
    load_regression_corpus(&corpus_root())
}

fn load_regression_corpus(
    root: &Path,
) -> Result<Vec<RegressionCorpusEntry>, RegressionCorpusInventoryError> {
    let directory = fs::read_dir(root).map_err(|source| RegressionCorpusInventoryError::Io {
        path: root.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let mut entries = Vec::new();
    for directory_entry in directory {
        let directory_entry =
            directory_entry.map_err(|source| RegressionCorpusInventoryError::Io {
                path: root.to_path_buf(),
                operation: "enumerated",
                source,
            })?;
        let path = directory_entry.path();
        let file_name = directory_entry.file_name();
        let file_type =
            directory_entry
                .file_type()
                .map_err(|source| RegressionCorpusInventoryError::Io {
                    path: path.clone(),
                    operation: "classified",
                    source,
                })?;
        if file_name == "README.md" {
            if !file_type.is_file() {
                return Err(RegressionCorpusInventoryError::UnsupportedEntry(path));
            }
            continue;
        }
        if !file_type.is_file()
            || path.extension().and_then(|extension| extension.to_str()) != Some("json")
        {
            return Err(RegressionCorpusInventoryError::UnsupportedEntry(path));
        }

        let file = fs::File::open(&path).map_err(|source| RegressionCorpusInventoryError::Io {
            path: path.clone(),
            operation: "opened",
            source,
        })?;
        let read_limit = u64::try_from(REGRESSION_CORPUS_MAX_ENTRY_BYTES)
            .expect("corpus entry bound should fit u64")
            .saturating_add(1);
        let mut bytes = Vec::new();
        file.take(read_limit)
            .read_to_end(&mut bytes)
            .map_err(|source| RegressionCorpusInventoryError::Io {
                path: path.clone(),
                operation: "read",
                source,
            })?;
        if bytes.len() > REGRESSION_CORPUS_MAX_ENTRY_BYTES {
            return Err(RegressionCorpusInventoryError::EntryTooLarge {
                path,
                observed_bytes: bytes.len(),
                maximum_bytes: REGRESSION_CORPUS_MAX_ENTRY_BYTES,
            });
        }
        let entry =
            RegressionCorpusEntry::from_canonical_json(bytes.as_slice()).map_err(|source| {
                RegressionCorpusInventoryError::Decode {
                    path: path.clone(),
                    source,
                }
            })?;
        let expected_file_name = format!("{}.json", entry.regression_id());
        if file_name.to_str() != Some(expected_file_name.as_str()) {
            return Err(RegressionCorpusInventoryError::FilenameMismatch {
                path,
                regression_id: entry.regression_id().to_string(),
            });
        }
        entries.push(entry);
    }
    entries.sort_by(|left, right| left.regression_id().cmp(right.regression_id()));

    Ok(entries)
}

/// Typed failure while enumerating the sole current checked-in corpus.
#[derive(Debug)]
pub enum RegressionCorpusInventoryError {
    /// A canonical entry failed current-format decoding or validation.
    Decode {
        /// Corpus path that failed decoding.
        path: PathBuf,
        /// Original generator-owned decoding cause.
        source: SqlGeneratorError,
    },

    /// A bounded read proved the entry exceeds the current byte limit.
    EntryTooLarge {
        /// Oversized corpus path.
        path: PathBuf,
        /// Observed byte count, capped at one byte beyond the limit.
        observed_bytes: usize,
        /// Maximum current entry byte count.
        maximum_bytes: usize,
    },

    /// The JSON filename did not equal the embedded reviewed identity.
    FilenameMismatch {
        /// Mismatched corpus path.
        path: PathBuf,
        /// Embedded reviewed regression identity.
        regression_id: String,
    },

    /// Corpus directory or file I/O failed with its original cause.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Stable operation label.
        operation: &'static str,
        /// Original filesystem cause.
        source: io::Error,
    },

    /// The corpus contained a non-JSON file, directory, or symbolic link.
    UnsupportedEntry(PathBuf),
}

impl Display for RegressionCorpusInventoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { path, .. } => {
                write!(
                    formatter,
                    "failed to decode corpus entry {}",
                    path.display()
                )
            }
            Self::EntryTooLarge {
                path,
                observed_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "corpus entry {} has at least {observed_bytes} bytes, exceeding the {maximum_bytes}-byte bound",
                path.display(),
            ),
            Self::FilenameMismatch {
                path,
                regression_id,
            } => write!(
                formatter,
                "corpus entry {} does not match embedded regression ID {regression_id:?}",
                path.display(),
            ),
            Self::Io {
                path, operation, ..
            } => write!(
                formatter,
                "corpus path {} could not be {operation}",
                path.display(),
            ),
            Self::UnsupportedEntry(path) => write!(
                formatter,
                "unsupported entry in regression corpus: {}",
                path.display(),
            ),
        }
    }
}

impl Error for RegressionCorpusInventoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

#[test]
fn checked_in_regression_corpus_is_current_and_complete() {
    let entries = checked_in_regression_corpus().expect("checked-in corpus should validate");

    assert_eq!(
        entries
            .iter()
            .map(RegressionCorpusEntry::regression_id)
            .collect::<Vec<_>>(),
        vec!["select.filtered-global-count-residual-scan"],
        "the checked-in inventory must name every reviewed regression exactly once",
    );
}

#[test]
fn corpus_inventory_rejects_unsupported_and_oversized_entries_before_decode() {
    let root = std::env::temp_dir().join(format!(
        "icydb-regression-corpus-policy-{}",
        std::process::id(),
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("temporary corpus directory should create");

    let unsupported = root.join("unsupported.csv");
    fs::write(&unsupported, b"stale").expect("unsupported fixture should write");
    assert!(matches!(
        load_regression_corpus(&root),
        Err(RegressionCorpusInventoryError::UnsupportedEntry(path)) if path == unsupported
    ));
    fs::remove_file(&unsupported).expect("unsupported fixture should remove");

    let oversized = root.join("oversized.json");
    let file = fs::File::create(&oversized).expect("oversized fixture should create");
    file.set_len(
        u64::try_from(REGRESSION_CORPUS_MAX_ENTRY_BYTES)
            .expect("corpus bound should fit u64")
            .saturating_add(1),
    )
    .expect("oversized fixture should extend");
    assert!(matches!(
        load_regression_corpus(&root),
        Err(RegressionCorpusInventoryError::EntryTooLarge { path, .. }) if path == oversized
    ));

    fs::remove_dir_all(&root).expect("temporary corpus directory should remove");
}
