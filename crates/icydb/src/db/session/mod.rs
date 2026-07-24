//! Module: db::session
//!
//! Responsibility: public session and fluent query facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

mod catalog;
pub mod delete;
pub(crate) mod generated;
mod integrity;
pub mod load;
mod macros;
#[cfg(feature = "sql")]
mod sql;
mod write;

use crate::{db::query::MissingRowPolicy, metrics::MetricsSink, traits::CanisterKind};

use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use integrity::IntegrityCheckError;
#[cfg(feature = "sql")]
pub use integrity::SqlIntegrityError;
pub use load::{FluentLoadQuery, PartialWindowLoadQuery};
#[cfg(feature = "sql")]
pub use sql::{
    SqlExecutionPerfAttribution, SqlPureCoveringPerfAttribution, SqlQueryPerfAttribution,
};
pub use write::{MutationMode, StructuralPatch};

///
/// DbSession
///
/// Public facade for session-scoped query execution, typed SQL lowering, and
/// structural mutation policy.
/// Wraps the core session and converts core results and errors into the
/// outward-facing `icydb` response surface.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    // ------------------------------------------------------------------
    // Session configuration
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn new(session: core::db::DbSession<C>) -> Self {
        Self { inner: session }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    #[must_use]
    pub fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    // ------------------------------------------------------------------
    // Query entry points
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
    {
        FluentLoadQuery {
            inner: self.inner.load::<E>(),
        }
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> FluentLoadQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
    {
        FluentLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete::<E>(),
        }
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> SessionDeleteQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete_with_consistency::<E>(consistency),
        }
    }
}
