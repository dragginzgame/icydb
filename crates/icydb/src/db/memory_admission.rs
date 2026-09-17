//! Module: db::memory_admission
//!
//! Responsibility: admit logical IcyDB allocation declarations before bootstrap commits.
//! Does not own: placement, persistence, memory handles, schema or store retirement.
//! Boundary: recovered allocation metadata to the host's existing bootstrap policy.

use std::{collections::BTreeMap, error::Error, fmt};

use ic_memory::{BootstrapAdmission, BootstrapAdmissionError, StableKey};

/// Rejection of IcyDB's logical allocation set before allocation commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryBootstrapAdmissionError {
    /// A key in the reserved `icydb.` domain does not name a current allocation role.
    UnsupportedKey(String),
    /// IcyDB roles require logical requests with authority `icydb.<namespace>`.
    InvalidDeclaration(String),
    /// A current namespace lacks controls, or a current store lacks its quartet.
    IncompleteRoles {
        /// Durable database namespace.
        namespace: String,
        /// Durable store key, or `None` for the namespace's controls.
        store: Option<String>,
    },
    /// Historical database identity must remain in the original current declarations.
    NamespaceRemoved(String),
    /// A historical journal could not be selected under current host authority.
    HistoricalJournal(BootstrapAdmissionError),
}

impl fmt::Display for MemoryBootstrapAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKey(key) => write!(f, "unsupported IcyDB allocation key: {key}"),
            Self::InvalidDeclaration(key) => {
                write!(
                    f,
                    "IcyDB allocation requires a namespace-authorized logical request: {key}"
                )
            }
            Self::IncompleteRoles { namespace, store } => {
                write!(f, "incomplete IcyDB allocation roles for {namespace}")?;
                if let Some(store) = store {
                    write!(f, " store {store}")?;
                }
                Ok(())
            }
            Self::NamespaceRemoved(namespace) => {
                write!(f, "historical IcyDB namespace is not declared: {namespace}")
            }
            Self::HistoricalJournal(error) => error.fmt(f),
        }
    }
}

impl Error for MemoryBootstrapAdmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HistoricalJournal(error) => Some(error),
            _ => None,
        }
    }
}

/// Admit logical IcyDB allocations and retain omitted journals for recovery checks.
///
/// A composed host calls this once from `RuntimeBootstrapPolicy::prepare_bootstrap`
/// and propagates its error. Its policy identity must cover these admission semantics.
/// This examines the whole sealed snapshot, so a host calls it once for all IcyDB
/// namespaces, not once per database. It neither grants memory ranges nor opens memory.
///
/// The original requests must contain all three controls for every historical
/// namespace and all four roles for each current store. Only omitted journal
/// allocations are added; other consumers' keys are left alone. Current grants
/// and the host's final policy still apply to every selected allocation.
///
/// Store keys are permanent identities. Changing one means remove/add, not a
/// rename or data transfer. Journal debt, pending commits, accepted schema and
/// database retirement are checked by IcyDB after allocation bootstrap succeeds.
/// Warm adoption cannot run this hook retroactively or append declarations.
///
/// # Errors
///
/// Rejects unsupported IcyDB keys, non-logical or incorrectly authorized current
/// declarations, incomplete role sets, omitted historical namespaces, and failed
/// historical-journal selection. Propagate rejection to abort this bootstrap.
pub fn prepare_memory_bootstrap(
    admission: &mut BootstrapAdmission<'_>,
) -> Result<(), MemoryBootstrapAdmissionError> {
    // Group only the original sealed requests. Another consumer's historical
    // selections must not manufacture current controls or a current store.
    let mut groups = BTreeMap::<(&str, Option<&str>), u8>::new();
    for registration in admission.declarations().registered_declarations() {
        let key = registration.declaration().stable_key();
        if allocation_role(key)?.is_some() {
            return Err(MemoryBootstrapAdmissionError::InvalidDeclaration(
                key.as_str().to_owned(),
            ));
        }
    }
    for request in admission.declarations().requests() {
        let Some(role) = allocation_role(request.stable_key())? else {
            continue;
        };
        if request.authority().strip_prefix("icydb.") != Some(role.namespace) {
            return Err(MemoryBootstrapAdmissionError::InvalidDeclaration(
                request.stable_key().as_str().to_owned(),
            ));
        }
        *groups.entry((role.namespace, role.store)).or_default() |= role.bit;
    }
    for (&(namespace, store), &roles) in &groups {
        if !groups.contains_key(&(namespace, None)) {
            return Err(incomplete_roles(namespace, None));
        }
        let required = if store.is_some() { 0b1111 } else { 0b111 };
        if roles != required {
            return Err(incomplete_roles(namespace, store));
        }
    }

    // Recovery already bounds this metadata to 255 records. Copy only omitted
    // journal identities so selection can mutably borrow admission afterward.
    let mut journals = Vec::new();
    for record in admission.recovered_allocations() {
        let Some(role) = allocation_role(record.stable_key)? else {
            continue;
        };
        if !groups.contains_key(&(role.namespace, None)) {
            return Err(MemoryBootstrapAdmissionError::NamespaceRemoved(
                role.namespace.to_owned(),
            ));
        }
        if role.store.is_some()
            && role.bit == 0b1000
            && !groups.contains_key(&(role.namespace, role.store))
        {
            journals.push((
                format!("icydb.{}", role.namespace),
                record.stable_key.clone(),
            ));
        }
    }
    for (authority, key) in journals {
        // Known-only selection preserves placement and rejects generic retirement
        // or revoked grants. IcyDB retirement is a separate post-open decision.
        admission
            .include_historical(&authority, key.as_str())
            .map_err(MemoryBootstrapAdmissionError::HistoricalJournal)?;
    }
    Ok(())
}

/// A borrowed role identity; bit masks describe complete groups, not lifecycle state.
struct AllocationRole<'a> {
    namespace: &'a str,
    store: Option<&'a str>,
    bit: u8,
}

fn allocation_role(
    key: &StableKey,
) -> Result<Option<AllocationRole<'_>>, MemoryBootstrapAdmissionError> {
    // StableKey already validates segment syntax and the complete byte bound.
    // This owner checks only IcyDB's closed role grammar and current version.
    let mut parts = key.as_str().split('.');
    if parts.next() != Some("icydb") {
        return Ok(None);
    }
    let namespace = parts.next();
    let role = match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("commit"), Some("control"), Some("v1"), None, None) => Some((None, 0b001)),
        (Some("startup"), Some("control"), Some("v1"), None, None) => Some((None, 0b010)),
        (Some("integrity"), Some("progress"), Some("v1"), None, None) => Some((None, 0b100)),
        (Some("store"), Some(store), Some(role), Some("v1"), None) => match role {
            "data" => Some((Some(store), 0b0001)),
            "index" => Some((Some(store), 0b0010)),
            "schema" => Some((Some(store), 0b0100)),
            "journal" => Some((Some(store), 0b1000)),
            _ => None,
        },
        _ => None,
    };
    match (namespace, role) {
        (Some(namespace), Some((store, bit))) => Ok(Some(AllocationRole {
            namespace,
            store,
            bit,
        })),
        _ => Err(MemoryBootstrapAdmissionError::UnsupportedKey(
            key.as_str().to_owned(),
        )),
    }
}

fn incomplete_roles(namespace: &str, store: Option<&str>) -> MemoryBootstrapAdmissionError {
    MemoryBootstrapAdmissionError::IncompleteRoles {
        namespace: namespace.to_owned(),
        store: store.map(str::to_owned),
    }
}
