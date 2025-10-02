#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct DiagnosticsOptions {}

impl DiagnosticsOptions {
    pub fn new() -> Self {
        Self::default()
    }
}
