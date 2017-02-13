#[derive(Debug)]
pub enum ViewResult {
    Meta(ViewMeta),
    Row(ViewRow),
}

#[derive(Debug)]
pub struct ViewMeta {
    pub inner: String,
}

#[derive(Debug)]
pub struct ViewRow {
    pub id: String,
    pub value: String,
    pub key: String,
}
