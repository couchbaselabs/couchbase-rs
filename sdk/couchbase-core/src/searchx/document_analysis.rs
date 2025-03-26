use crate::searchx::search_json::DocumentAnalysisJson;

pub struct DocumentAnalysis {
    pub status: String,
    pub analyzed: Vec<u8>,
}

impl From<DocumentAnalysisJson> for DocumentAnalysis {
    fn from(value: DocumentAnalysisJson) -> DocumentAnalysis {
        DocumentAnalysis {
            status: value.status,
            analyzed: value.analyzed,
        }
    }
}
