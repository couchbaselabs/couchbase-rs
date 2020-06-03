use serde_json::json;

pub trait SearchQuery {
    fn to_json(&self) -> serde_json::Value;
}

pub struct QueryStringQuery {
    query: String,
}

impl QueryStringQuery {
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl SearchQuery for QueryStringQuery {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "query": &self.query.clone(),
        })
    }
}
