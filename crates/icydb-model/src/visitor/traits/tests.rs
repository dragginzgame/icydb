use super::*;
use crate::{
    normalize::normalize,
    validate::validate,
    visitor::{ApplicationOperation, CallbackKind, Issue, VisitorError},
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

const AUTO_NORMALIZE_ISSUE: &str = "automatic normalize";
const CUSTOM_NORMALIZE_ISSUE: &str = "custom normalize";
const AUTO_VALIDATE_ISSUE: &str = "automatic validate";
const CUSTOM_VALIDATE_ISSUE: &str = "custom validate";

#[derive(Default)]
struct HookProbe {
    auto_normalize: u32,
    custom_normalize: u32,
    auto_validate: Cell<u32>,
    custom_validate: Cell<u32>,
}

struct OrderedLeaf {
    events: Rc<RefCell<Vec<&'static str>>>,
}

impl Visitable for OrderedLeaf {}

impl NormalizeAuto for OrderedLeaf {
    fn normalize_self(&mut self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("leaf normalize auto");
    }
}

impl NormalizeCustom for OrderedLeaf {
    fn normalize_custom(&mut self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("leaf normalize custom");
    }
}

impl ValidateAuto for OrderedLeaf {
    fn validate_self(&self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("leaf validate auto");
    }
}

impl ValidateCustom for OrderedLeaf {
    fn validate_custom(&self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("leaf validate custom");
    }
}

struct OrderedParent {
    events: Rc<RefCell<Vec<&'static str>>>,
    child: OrderedLeaf,
}

impl Visitable for OrderedParent {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        perform_visit(visitor, &self.child, "child");
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        perform_visit_mut(visitor, &mut self.child, "child");
    }
}

impl NormalizeAuto for OrderedParent {
    fn normalize_self(&mut self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("parent normalize auto");
    }
}

impl NormalizeCustom for OrderedParent {
    fn normalize_custom(&mut self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("parent normalize custom");
    }
}

impl ValidateAuto for OrderedParent {
    fn validate_self(&self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("parent validate auto");
    }
}

impl ValidateCustom for OrderedParent {
    fn validate_custom(&self, _ctx: &mut dyn VisitorContext) {
        self.events.borrow_mut().push("parent validate custom");
    }
}

fn ordered_parent() -> (OrderedParent, Rc<RefCell<Vec<&'static str>>>) {
    let events = Rc::new(RefCell::new(Vec::new()));
    (
        OrderedParent {
            events: Rc::clone(&events),
            child: OrderedLeaf {
                events: Rc::clone(&events),
            },
        },
        events,
    )
}

impl Visitable for HookProbe {}

impl NormalizeAuto for HookProbe {
    fn normalize_self(&mut self, ctx: &mut dyn VisitorContext) {
        self.auto_normalize += 1;
        ctx.issue(AUTO_NORMALIZE_ISSUE);
    }
}

impl NormalizeCustom for HookProbe {
    fn normalize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        self.custom_normalize += 1;
        ctx.issue(CUSTOM_NORMALIZE_ISSUE);
    }
}

impl ValidateAuto for HookProbe {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        self.auto_validate.set(self.auto_validate.get() + 1);
        ctx.issue(AUTO_VALIDATE_ISSUE);
    }
}

impl ValidateCustom for HookProbe {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        self.custom_validate.set(self.custom_validate.get() + 1);
        ctx.issue(CUSTOM_VALIDATE_ISSUE);
    }
}

fn assert_issues(error: &VisitorError, path: &str, expected: [&str; 2]) {
    let issues = error
        .issues()
        .get(path)
        .unwrap_or_else(|| panic!("expected visitor issues at {path}"));
    let messages = issues.iter().map(Issue::message).collect::<Vec<_>>();
    assert_eq!(messages, expected);
}

fn assert_callbacks(error: &VisitorError, path: &str, expected: [CallbackKind; 2]) {
    let issues = error
        .issues()
        .get(path)
        .unwrap_or_else(|| panic!("expected visitor issues at {path}"));
    let callbacks = issues
        .iter()
        .map(|issue| {
            issue
                .callback()
                .expect("top-level application traversal must type every callback")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        callbacks
            .iter()
            .map(|callback| callback.kind())
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        callbacks
            .iter()
            .all(|callback| callback.type_path() == std::any::type_name::<HookProbe>())
    );
}

#[test]
fn option_vec_normalize_hooks_run_once_at_each_indexed_path() {
    let mut value = Some(vec![HookProbe::default(), HookProbe::default()]);

    let error = normalize(&mut value).expect_err("probe normalizers should report issues");
    assert_eq!(error.operation(), ApplicationOperation::Normalize);

    let Some(probes) = value.as_ref() else {
        panic!("normalize should preserve the populated option");
    };
    for probe in probes {
        assert_eq!(probe.auto_normalize, 1);
        assert_eq!(probe.custom_normalize, 1);
    }
    assert!(error.issues().get("").is_none());
    assert_issues(
        &error,
        "[0]",
        [AUTO_NORMALIZE_ISSUE, CUSTOM_NORMALIZE_ISSUE],
    );
    assert_callbacks(
        &error,
        "[0]",
        [CallbackKind::NormalizeAuto, CallbackKind::NormalizeCustom],
    );
    assert_issues(
        &error,
        "[1]",
        [AUTO_NORMALIZE_ISSUE, CUSTOM_NORMALIZE_ISSUE],
    );
}

#[test]
fn option_vec_validate_hooks_run_once_at_each_indexed_path() {
    let value = Some(vec![HookProbe::default(), HookProbe::default()]);

    let error = validate(&value).expect_err("probe validators should report issues");
    assert_eq!(error.operation(), ApplicationOperation::Validate);

    let Some(probes) = value.as_ref() else {
        panic!("validate should preserve the populated option");
    };
    for probe in probes {
        assert_eq!(probe.auto_validate.get(), 1);
        assert_eq!(probe.custom_validate.get(), 1);
    }
    assert!(error.issues().get("").is_none());
    assert_issues(&error, "[0]", [AUTO_VALIDATE_ISSUE, CUSTOM_VALIDATE_ISSUE]);
    assert_callbacks(
        &error,
        "[0]",
        [CallbackKind::ValidateAuto, CallbackKind::ValidateCustom],
    );
    assert_issues(&error, "[1]", [AUTO_VALIDATE_ISSUE, CUSTOM_VALIDATE_ISSUE]);
}

#[test]
fn box_transparency_keeps_one_forwarded_hook_call() {
    let mut normalized = Box::new(HookProbe::default());
    let normalize_error =
        normalize(&mut normalized).expect_err("probe normalizers should report issues");
    assert_eq!(normalized.auto_normalize, 1);
    assert_eq!(normalized.custom_normalize, 1);
    assert_callbacks(
        &normalize_error,
        "",
        [CallbackKind::NormalizeAuto, CallbackKind::NormalizeCustom],
    );

    let validated = Box::new(HookProbe::default());
    let validate_error = validate(&validated).expect_err("probe validators should report issues");
    assert_eq!(validated.auto_validate.get(), 1);
    assert_eq!(validated.custom_validate.get(), 1);
    assert_callbacks(
        &validate_error,
        "",
        [CallbackKind::ValidateAuto, CallbackKind::ValidateCustom],
    );
}

#[test]
fn normalize_and_validate_traversals_are_preorder_and_declaration_ordered() {
    let (mut normalized, normalize_events) = ordered_parent();
    normalize(&mut normalized).expect("ordered normalizers should succeed");
    assert_eq!(
        normalize_events.borrow().as_slice(),
        [
            "parent normalize auto",
            "parent normalize custom",
            "leaf normalize auto",
            "leaf normalize custom",
        ]
    );

    let (validated, validate_events) = ordered_parent();
    validate(&validated).expect("ordered validators should succeed");
    assert_eq!(
        validate_events.borrow().as_slice(),
        [
            "parent validate auto",
            "parent validate custom",
            "leaf validate auto",
            "leaf validate custom",
        ]
    );
}
