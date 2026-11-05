use super::*;

#[test]
fn access_projection_labels_only_need_composite_root_and_child_count() {
    let children = [(), (), ()];
    let mut output = String::new();
    let mut projection = AccessStrategyLabelProjection { out: &mut output };
    AccessPlanProjection::<Value>::union(&mut projection, &children, |(), _| {
        panic!("label must not inspect child payloads")
    })
    .unwrap();
    assert_eq!(output, "Union(3)");
    output.clear();
    let mut projection = AccessStrategyLabelProjection { out: &mut output };
    AccessPlanProjection::<Value>::intersection(&mut projection, &children, |(), _| {
        panic!("label must not inspect child payloads")
    })
    .unwrap();
    assert_eq!(output, "Intersection(3)");
}
