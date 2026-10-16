//! Independent Candid field/type expectations for the shared audit wire records.

use crate::contracts::{
    AcceptedSchemaReadInstructionResult, ApplicationStartupHook, ApplicationStartupSnapshot,
    SchemaApplicationPerfResult, StartupObservationPerfResult, StartupWatchdogPerfSnapshot,
};
use candid::{
    CandidType,
    types::{Field, Label, Type, TypeInner},
};

// Spell wire widths independently of the Rust records so moving both endpoints
// onto one declaration does not make an accidental contract change invisible.
fn record(fields: &[(&str, Type)]) -> Type {
    let mut fields = fields
        .iter()
        .map(|(name, ty)| Field {
            id: Label::Named((*name).to_string()).into(),
            ty: ty.clone(),
        })
        .collect::<Vec<_>>();
    fields.sort_by_key(|field| field.id.get_id());
    TypeInner::Record(fields).into()
}

#[test]
fn schema_and_startup_observation_wire_shapes_are_unchanged() {
    assert_eq!(
        SchemaApplicationPerfResult::ty(),
        record(&[
            ("local_instructions", TypeInner::Nat64.into()),
            ("reconcile_checks", TypeInner::Nat64.into()),
            ("first_create", TypeInner::Nat64.into()),
            ("exact_match", TypeInner::Nat64.into()),
        ])
    );
    assert_eq!(
        StartupObservationPerfResult::ty(),
        record(&[
            ("state", icydb::db::DatabaseStartupState::ty()),
            ("local_instructions", TypeInner::Nat64.into()),
        ])
    );
    assert_eq!(
        AcceptedSchemaReadInstructionResult::ty(),
        record(&[
            ("description", icydb::db::EntitySchemaDescription::ty()),
            ("local_instructions", TypeInner::Nat64.into()),
        ])
    );
}

#[test]
fn watchdog_wire_shape_preserves_unbounded_nat_totals_and_optional_samples() {
    let optional_sample: Type = TypeInner::Opt(TypeInner::Nat64.into()).into();
    assert_eq!(
        StartupWatchdogPerfSnapshot::ty(),
        record(&[
            ("scheduler_samples", TypeInner::Nat64.into()),
            ("scheduler_total_instructions", TypeInner::Nat.into()),
            ("scheduler_maximum_instructions", optional_sample.clone()),
            ("work_samples", TypeInner::Nat64.into()),
            ("work_total_instructions", TypeInner::Nat.into()),
            ("work_latest_instructions", optional_sample.clone()),
            ("work_maximum_instructions", optional_sample),
            ("work_started", TypeInner::Nat64.into()),
            ("work_completed", TypeInner::Nat64.into()),
            ("succeeded", TypeInner::Nat64.into()),
            ("no_work", TypeInner::Nat64.into()),
            ("retryable_failures", TypeInner::Nat64.into()),
            ("invariant_failures", TypeInner::Nat64.into()),
        ])
    );
}

#[test]
fn application_startup_wire_shape_preserves_hooks_and_failure() {
    let mut hooks = vec![
        Field {
            id: Label::Named("Init".into()).into(),
            ty: TypeInner::Null.into(),
        },
        Field {
            id: Label::Named("PostUpgrade".into()).into(),
            ty: TypeInner::Null.into(),
        },
    ];
    hooks.sort_by_key(|field| field.id.get_id());
    assert_eq!(
        ApplicationStartupHook::ty(),
        TypeInner::Variant(hooks).into()
    );
    assert_eq!(
        ApplicationStartupSnapshot::ty(),
        record(&[
            ("hook", ApplicationStartupHook::ty()),
            ("engine_registered_before_hook", TypeInner::Bool.into()),
            ("observations", TypeInner::Nat32.into()),
            ("recovering_observations", TypeInner::Nat32.into()),
            ("ready_observations", TypeInner::Nat32.into()),
            ("restorations", TypeInner::Nat32.into()),
            ("retry_scheduled", TypeInner::Bool.into()),
            (
                "failure",
                TypeInner::Opt(icydb::db::StartupFailure::ty()).into()
            ),
        ])
    );
}
