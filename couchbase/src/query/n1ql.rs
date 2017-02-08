#[derive(Debug)]
pub enum N1qlResult {
    Row(String),
    Meta(String),
}
