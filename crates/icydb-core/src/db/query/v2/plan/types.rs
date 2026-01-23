#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderSpec {
    pub fields: Vec<(String, OrderDirection)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}
