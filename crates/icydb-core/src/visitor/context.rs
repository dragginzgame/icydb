///
/// VisitorContext
/// Narrow interface exposed to visitors for reporting non-fatal issues.
/// Implemented by adapters via a short-lived context object.
///

pub trait VisitorContext {
    fn add_issue(&mut self, issue: Issue);
    fn add_issue_at(&mut self, seg: PathSegment, issue: Issue);
}

impl dyn VisitorContext + '_ {
    pub fn issue(&mut self, msg: impl Into<String>) {
        self.add_issue(Issue {
            message: msg.into(),
        });
    }

    pub fn issue_at(&mut self, seg: PathSegment, msg: impl Into<String>) {
        self.add_issue_at(
            seg,
            Issue {
                message: msg.into(),
            },
        );
    }
}

/// VisitorContext that pins all issues to a single path segment.
pub struct ScopedContext<'a> {
    ctx: &'a mut dyn VisitorContext,
    seg: PathSegment,
}

impl<'a> ScopedContext<'a> {
    #[must_use]
    pub fn new(ctx: &'a mut dyn VisitorContext, seg: PathSegment) -> Self {
        Self { ctx, seg }
    }
}

impl VisitorContext for ScopedContext<'_> {
    fn add_issue(&mut self, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }

    fn add_issue_at(&mut self, _seg: PathSegment, issue: Issue) {
        self.ctx.add_issue_at(self.seg.clone(), issue);
    }
}

///
/// Issue
///

#[derive(Clone, Debug, Default)]
pub struct Issue {
    pub message: String,
}

///
/// PathSegment
///

#[derive(Clone, Debug)]
pub enum PathSegment {
    Empty,
    Field(&'static str),
    Index(usize),
}

impl From<&'static str> for PathSegment {
    fn from(s: &'static str) -> Self {
        Self::Field(s)
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Index(i)
    }
}

impl From<Option<&'static str>> for PathSegment {
    fn from(opt: Option<&'static str>) -> Self {
        match opt {
            Some(s) if !s.is_empty() => Self::Field(s),
            _ => Self::Empty,
        }
    }
}
