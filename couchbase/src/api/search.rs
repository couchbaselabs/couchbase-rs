
use serde_json::{json};
use serde_derive::{Serialize};
use std::fmt::Debug;
use serde::{Serialize};


// TODO: Is this weird?
pub trait SearchQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error>;
}

#[derive(Debug)]
pub struct QueryStringQuery {
    query: String,
    boost: Option<f32>,
}

impl QueryStringQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self { query: query.into(), boost: None }
    }

    pub fn boost(mut self, boost: f32) -> QueryStringQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for QueryStringQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "query": &self.query.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct MatchQuery {
    match_query: String,
    boost: Option<f32>,
    field: Option<String>,
    analyzer: Option<String>,
    prefix_length: Option<u64>,
    fuzziness: Option<u64>,
}

impl MatchQuery {
    pub fn new(match_query: impl Into<String>) -> Self {
        Self {
            match_query: match_query.into(),
            boost: None,
            field: None,
            analyzer: None,
            prefix_length: None,
            fuzziness: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> MatchQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> MatchQuery {
        self.field = Some(field.into());
        self
    }

    pub fn analyzer(mut self, analyzer: impl Into<String>) -> MatchQuery {
        self.analyzer = Some(analyzer.into());
        self
    }

    pub fn prefix_length(mut self, length: u64) -> MatchQuery {
        self.prefix_length = Some(length);
        self
    }

    pub fn fuzziness(mut self, fuzziness: u64) -> MatchQuery {
        self.fuzziness = Some(fuzziness);
        self
    }
}

impl SearchQuery for MatchQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "match": &self.match_query.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.analyzer.clone() {
            v["analyzer"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.prefix_length {
            v["prefix_length"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.fuzziness {
            v["fuzziness"] = serde_json::to_value(val)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct MatchPhraseQuery {
    match_phrase: String,
    boost: Option<f32>,
    field: Option<String>,
    analyzer: Option<String>,
}

impl MatchPhraseQuery {
    pub fn new(phrase: String) -> Self {
        Self {
            match_phrase: phrase,
            boost: None,
            field: None,
            analyzer: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> MatchPhraseQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> MatchPhraseQuery {
        self.field = Some(field.into());
        self
    }

    pub fn analyzer(mut self, analyzer: impl Into<String>) -> MatchPhraseQuery {
        self.analyzer = Some(analyzer.into());
        self
    }
}

impl SearchQuery for MatchPhraseQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "match_phrase": &self.match_phrase.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.analyzer.clone() {
            v["analyzer"] = serde_json::to_value(val)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct RegexpQuery {
    regexp: String,
    boost: Option<f32>,
    field: Option<String>,
}

impl RegexpQuery {
    pub fn new(regexp: impl Into<String>) -> Self {
        Self {
            regexp: regexp.into(),
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> RegexpQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> RegexpQuery {
        self.field = Some(field.into());
        self
    }
}

impl SearchQuery for RegexpQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "regexp": &self.regexp.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct NumericRangeQuery {
    min: Option<f32>,
    max: Option<f32>,
    inclusive_min: Option<bool>,
    inclusive_max: Option<bool>,
    boost: Option<f32>,
    field: Option<String>,
}

impl NumericRangeQuery {
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,
            inclusive_min: None,
            inclusive_max: None,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> NumericRangeQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> NumericRangeQuery {
        self.field = Some(field.into());
        self
    }

    pub fn min(mut self, min: f32, inclusive: bool) -> NumericRangeQuery {
        self.min = Some(min);
        self.inclusive_min = Some(inclusive);
        self
    }

    pub fn max(mut self, max: f32, inclusive: bool) -> NumericRangeQuery {
        self.max = Some(max);
        self.inclusive_max = Some(inclusive);
        self
    }
}

impl SearchQuery for NumericRangeQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({});
        if let Some(val) = self.min {
            v["min"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.max {
            v["max"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_min {
            v["inclusive_min"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_max {
            v["inclusive_max"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct DateRangeQuery {
    start: Option<String>,
    end: Option<String>,
    inclusive_start: Option<bool>,
    inclusive_end: Option<bool>,
    datetime_parser: Option<String>,
    boost: Option<f32>,
    field: Option<String>,
}

impl DateRangeQuery {
    pub fn new() -> Self {
        Self {
            start: None,
            end: None,
            inclusive_start: None,
            inclusive_end: None,
            datetime_parser: None,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> DateRangeQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> DateRangeQuery {
        self.field = Some(field.into());
        self
    }

    pub fn start(mut self, start: String, inclusive: bool) -> DateRangeQuery {
        self.start = Some(start);
        self.inclusive_start = Some(inclusive);
        self
    }

    pub fn max(mut self, end: String, inclusive: bool) -> DateRangeQuery {
        self.end = Some(end);
        self.inclusive_end = Some(inclusive);
        self
    }

    pub fn datetime_parser(mut self, parser: impl Into<String>) -> DateRangeQuery {
        self.datetime_parser = Some(parser.into());
        self
    }
}

impl SearchQuery for DateRangeQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({});
        if let Some(val) = self.start.clone() {
            v["start"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.end.clone() {
            v["end"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_start {
            v["inclusive_start"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_end {
            v["inclusive_end"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.datetime_parser.clone() {
            v["datetime_parser"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        Ok(v)
    }
}

pub struct ConjunctionQuery {
    conjuncts: Vec<Box<dyn SearchQuery>>,
    boost: Option<f32>,
}

impl ConjunctionQuery {
    pub fn new(queries: Vec<Box<dyn SearchQuery>>) -> Self {
        Self {
            conjuncts: queries,
            boost: None,
        }
    }

    pub fn and(mut self, queries: Vec<Box<dyn SearchQuery>>) -> ConjunctionQuery {
        self.conjuncts.extend(queries);
        self
    }

    pub fn boost(mut self, boost: f32) -> ConjunctionQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for ConjunctionQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({});
        let mut queries = vec![];
        for query in &self.conjuncts {
            queries.push(query.to_json()?);
        }
        v["conjuncts"] = serde_json::to_value(queries)?;
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

pub struct DisjunctionQuery {
    disjuncts: Vec<Box<dyn SearchQuery>>,
    boost: Option<f32>,
    min: Option<u32>,
}

impl DisjunctionQuery {
    pub fn new(queries: Vec<Box<dyn SearchQuery>>) -> Self {
        Self {
            disjuncts: queries,
            boost: None,
            min: None,
        }
    }

    pub fn or(mut self, queries: Vec<Box<dyn SearchQuery>>) -> DisjunctionQuery {
        self.disjuncts.extend(queries);
        self
    }

    pub fn boost(mut self, boost: f32) -> DisjunctionQuery {
        self.boost = Some(boost);
        self
    }

    pub fn min(mut self, min: u32) -> DisjunctionQuery {
        self.min = Some(min);
        self
    }
}

impl SearchQuery for DisjunctionQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({});
        let mut queries = vec![];
        for query in &self.disjuncts {
            queries.push(query.to_json()?);
        }
        v["disjuncts"] = serde_json::to_value(queries)?;
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        if let Some(b) = self.min {
            v["min"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

pub struct BooleanQuery {
    must: Option<ConjunctionQuery>,
    must_not: Option<DisjunctionQuery>,
    should: Option<DisjunctionQuery>,

    should_min: Option<u32>,
    boost: Option<f32>,
}

impl BooleanQuery {
    pub fn new() -> Self {
        Self {
            must: None,
            must_not: None,
            should: None,
            should_min: None,
            boost: None,
        }
    }

    pub fn should_min(mut self, min_for_should: u32) -> BooleanQuery {
        self.should_min = Some(min_for_should);
        self
    }

    pub fn must(mut self, query: ConjunctionQuery) -> BooleanQuery {
        self.must = Some(query);
        self
    }

    pub fn must_not(mut self, query: DisjunctionQuery) -> BooleanQuery {
        self.must_not = Some(query);
        self
    }

    pub fn should(mut self, query: DisjunctionQuery) -> BooleanQuery {
        self.should = Some(query);
        self
    }

    pub fn boost(mut self, boost: f32) -> BooleanQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for BooleanQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({});
        if let Some(query) = &self.must {
            v["must"] = query.to_json()?;
        }
        if let Some(query) = &self.must_not {
            v["must_not"] = query.to_json()?;
        }
        if let Some(query) = &self.should {
            let mut payload = query.to_json()?;
            if let Some(min) = self.should_min {
                payload["min"] = serde_json::to_value(min)?;
            }
            v["should"] = payload;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct WildcardQuery {
    wildcard: String,

    field: Option<String>,
    boost: Option<f32>,
}

impl WildcardQuery {
    pub fn new(wildcard: String) -> Self {
        Self {
            wildcard,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> WildcardQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> WildcardQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for WildcardQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "wildcard": self.wildcard
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug, Serialize)]
pub struct DocIDQuery {
    ids: Vec<String>,

    field: Option<String>,
    boost: Option<f32>,
}

impl DocIDQuery {
    pub fn new<I, T>(doc_ids: Vec<String>) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>
    {
        Self {
            ids: doc_ids.into_iter().map(Into::into).collect::<Vec<String>>(),
            field: None,
            boost: None,
        }
    }

    pub fn add_doc_ids<I, T>(mut self, doc_ids: I) -> DocIDQuery
    where
        I: IntoIterator<Item = T>,
        T: Into<String>
    {
        self.ids.extend(doc_ids.into_iter().map(Into::into).collect::<Vec<String>>());
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> DocIDQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> DocIDQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for DocIDQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "ids": self.ids
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug, Serialize)]
pub struct BooleanFieldQuery {
    #[serde(rename = "bool")]
    val: bool,

    field: Option<String>,
    boost: Option<f32>,
}

impl BooleanFieldQuery {
    pub fn new(val: bool) -> Self {
        Self {
            val,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> BooleanFieldQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> BooleanFieldQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for BooleanFieldQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "bool": self.val
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug, Serialize)]
pub struct TermQuery {
    term: String,

    field: Option<String>,
    fuzziness: Option<u64>,
    prefix_length: Option<u64>,
    boost: Option<f32>,
}

impl TermQuery {
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            term: term.into(),
            field: None,
            fuzziness: None,
            prefix_length: None,
            boost: None,
        }
    }

    pub fn prefix_length(mut self, prefix_length: u64) -> TermQuery {
        self.prefix_length = Some(prefix_length);
        self
    }

    pub fn fuzziness(mut self, fuzziness: u64) -> TermQuery {
        self.fuzziness = Some(fuzziness);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> TermQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> TermQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for TermQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "term": self.term.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.prefix_length.clone() {
            v["prefix_length"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.fuzziness.clone() {
            v["fuzziness"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct PhraseQuery {
    terms: Vec<String>,

    field: Option<String>,
    boost: Option<f32>,
}

impl PhraseQuery {
    pub fn new<I, T>(terms: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>
    {
        Self {
            terms: terms.into_iter().map(Into::into).collect::<Vec<String>>(),
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> PhraseQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> PhraseQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for PhraseQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "terms": self.terms
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct PrefixQuery {
    prefix: String,

    field: Option<String>,
    boost: Option<f32>,
}

impl PrefixQuery {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> PrefixQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> PrefixQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for PrefixQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "prefix": self.prefix.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct MatchAllQuery {}

impl MatchAllQuery {
    pub fn new() -> Self {
        Self {}
    }
}

impl SearchQuery for MatchAllQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        Ok(json!({
            "match_all":  serde_json::Value::Null,
        }))
    }
}

#[derive(Debug)]
pub struct MatchNoneQuery {}

impl MatchNoneQuery {
    pub fn new() -> Self {
        Self {}
    }
}

impl SearchQuery for MatchNoneQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        Ok(json!({
            "match_none": serde_json::Value::Null,
        }))
    }
}

#[derive(Debug, Serialize)]
pub struct TermRangeQuery {
    term: String,

    field: Option<String>,
    boost: Option<f32>,
    min: Option<String>,
    max: Option<String>,
    inclusive_min: Option<bool>,
    inclusive_max: Option<bool>,
}

impl TermRangeQuery {
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            term: term.into(),
            field: None,
            boost: None,
            min: None,
            max: None,
            inclusive_min: None,
            inclusive_max: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> TermRangeQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> TermRangeQuery {
        self.boost = Some(boost);
        self
    }

    pub fn min(mut self, min: impl Into<String>, inclusive: bool) -> TermRangeQuery {
        self.min = Some(min.into());
        self.inclusive_min = Some(inclusive);
        self
    }

    pub fn max(mut self, max: impl Into<String>, inclusive: bool) -> TermRangeQuery {
        self.max = Some(max.into());
        self.inclusive_max = Some(inclusive);
        self
    }
}

impl SearchQuery for TermRangeQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "term": self.term.clone()
        });
        if let Some(val) = self.min.clone() {
            v["min"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.max.clone() {
            v["max"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_min {
            v["inclusive_min"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.inclusive_max {
            v["inclusive_max"] = serde_json::to_value(val)?;
        }
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct GeoDistanceQuery {
    location: [f64; 2],
    distance: String,

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoDistanceQuery {
    pub fn new(lon: f64, lat: f64, distance: impl Into<String>) -> Self {
        Self {
            location: [lon, lat],
            distance: distance.into(),
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> GeoDistanceQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoDistanceQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoDistanceQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "location": self.location,
            "distance": self.distance.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug)]
pub struct GeoBoundingBoxQuery {
    top_left: [f64; 2],
    bottom_right: [f64; 2],

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoBoundingBoxQuery {
    pub fn new(
        top_left_lon: f64,
        top_left_lat: f64,
        bottom_right_lon: f64,
        bottom_right_lat: f64,
    ) -> Self {
        Self {
            top_left: [top_left_lon, top_left_lat],
            bottom_right: [bottom_right_lon, bottom_right_lat],
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> GeoBoundingBoxQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoBoundingBoxQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoBoundingBoxQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut v = json!({
            "top_left": self.top_left,
            "bottom_right": self.bottom_right,
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Coordinate {
    lon: f64,
    lat: f64,
}

impl Coordinate {
    pub fn new(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

#[derive(Debug)]
pub struct GeoPolygonQuery {
    coordinates: Vec<Coordinate>,

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoPolygonQuery {
    pub fn new<I>(coordinates: I) -> Self
    where
        I: IntoIterator<Item = Coordinate>,
    {
        Self {
            coordinates: coordinates.into_iter().collect(),
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: impl Into<String>) -> GeoPolygonQuery {
        self.field = Some(field.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoPolygonQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoPolygonQuery {
    fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut points = vec![];
        for coord in &self.coordinates {
            points.push([coord.lon, coord.lat]);
        }
        let mut v = json!({
            "polygon_points": points,
        });
        if let Some(val) = self.field.clone() {
            v["field"] = serde_json::to_value(val)?;
        }
        if let Some(b) = self.boost {
            v["boost"] = serde_json::to_value(b)?;
        }
        Ok(v)
    }
}

pub trait SearchSort: Serialize {
}

#[derive(Debug, Serialize)]
pub struct SearchSortScore {
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<bool>,
    by: String,
}

impl SearchSortScore {
    pub fn desc(mut self, desc: bool) -> Self {
        self.desc = Some(desc);
        self
    }
}

impl Default for SearchSortScore {
    fn default() -> Self {
        Self {
            by: "score".into(),
            desc: None,
        }
    }
}

impl SearchSort for SearchSortScore {
}

#[derive(Debug, Serialize)]
pub struct SearchSortId {
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<bool>,
    by: String,
}

impl SearchSortId {
    pub fn desc(mut self, desc: bool) -> Self {
        self.desc = Some(desc);
        self
    }
}

impl Default for SearchSortId {
    fn default() -> Self {
        Self {
            by: "id".into(),
            desc: None,
        }
    }
}

impl SearchSort for SearchSortId {
}

#[derive(Debug, Serialize)]
pub enum SearchSortFieldType {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "number")]
    Number,
    #[serde(rename = "date")]
    Date,
}

#[derive(Debug, Serialize)]
pub enum SearchSortFieldMode {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
}

#[derive(Debug, Serialize)]
pub enum SearchSortFieldMissing {
    #[serde(rename = "first")]
    First,
    #[serde(rename = "last")]
    Last,
}

#[derive(Debug, Serialize)]
pub struct SearchSortField {
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<bool>,
    by: String,
    field: String,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    field_type: Option<SearchSortFieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<SearchSortFieldMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing: Option<SearchSortFieldMissing>,
}

impl SearchSortField {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            desc: None,
            by: "field".into(),
            field: field.into(),
            field_type: None,
            mode: None,
            missing: None
        }
    }
    pub fn desc(mut self, desc: bool) -> Self {
        self.desc = Some(desc);
        self
    }
    pub fn field_type(mut self, field_type: SearchSortFieldType) -> Self {
        self.field_type = Some(field_type);
        self
    }
    pub fn mode(mut self, mode: SearchSortFieldMode) -> Self {
        self.mode = Some(mode);
        self
    }
    pub fn missing(mut self, missing: SearchSortFieldMissing) -> Self {
        self.missing = Some(missing);
        self
    }
}

impl SearchSort for SearchSortField {
}

#[derive(Debug, Serialize)]
pub enum SearchSortGeoDistanceUnit {
    #[serde(rename = "meters")]
    Meters,
    #[serde(rename = "miles")]
    Miles,
    #[serde(rename = "centimeters")]
    Centimeters,
    #[serde(rename = "millimeters")]
    Millimeters,
    #[serde(rename = "nauticalmiles")]
    NauticalMiles,
    #[serde(rename = "kilometers")]
    Kilometers,
    #[serde(rename = "feet")]
    Feet,
    #[serde(rename = "yards")]
    Yards,
    #[serde(rename = "inch")]
    Inches,
}

#[derive(Debug, Serialize)]
pub struct SearchSortGeoDistance {
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<bool>,
    by: String,
    field: String,
    location: [f32;2],
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<SearchSortGeoDistanceUnit>,
}

impl SearchSortGeoDistance {
    pub fn new(field: impl Into<String>, location: [f32;2]) -> Self {
        Self {
            desc: None,
            by: "field".into(),
            field: field.into(),
            location,
            unit: None
        }
    }
    pub fn desc(mut self, desc: bool) -> Self {
        self.desc = Some(desc);
        self
    }
    pub fn unit(mut self, unit: SearchSortGeoDistanceUnit) -> Self {
        self.unit = Some(unit);
        self
    }
}

impl SearchSort for SearchSortGeoDistance {
}

pub trait SearchFacet: Serialize {
}

#[derive(Debug, Serialize)]
pub struct TermFacet {
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u32>,
}

impl TermFacet {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
        }
    }
    pub fn size(mut self, size: u32) -> Self {
        self.size = Some(size);
        self
    }
}

impl SearchFacet for TermFacet {}

#[derive(Debug, Serialize)]
pub struct SearchNumericRange {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    min: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<f32>,
}

impl SearchNumericRange {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            min: None,
            max: None,
        }
    }

    pub fn min(mut self, min: f32) -> Self {
        self.min = Some(min);
        self
    }

    pub fn max(mut self, max: f32) -> Self {
        self.max = Some(max);
        self
    }
}

#[derive(Debug, Serialize)]
pub struct NumericRangeFacet {
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u32>,
    numeric_ranges: Vec<SearchNumericRange>,
}

impl NumericRangeFacet {
    pub fn new<I>(field: impl Into<String>, numeric_ranges: I) -> Self
    where
        I: IntoIterator<Item = SearchNumericRange>
    {
        Self {
            field: field.into(),
            size: None,
            numeric_ranges: numeric_ranges.into_iter().collect()
        }
    }
    pub fn size(mut self, size: u32) -> Self {
        self.size = Some(size);
        self
    }
}

impl SearchFacet for NumericRangeFacet {}

#[derive(Debug, Serialize)]
pub struct SearchDateRange {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<String>,
}

impl SearchDateRange {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: None,
            end: None,
        }
    }

    pub fn start(mut self, start: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self
    }

    pub fn end(mut self, end: impl Into<String>) -> Self {
        self.end = Some(end.into());
        self
    }
}

#[derive(Debug, Serialize)]
pub struct DateRangeFacet {
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u32>,
    date_ranges: Vec<SearchDateRange>,
}

impl DateRangeFacet {
    pub fn new<I>(field: impl Into<String>, date_ranges: I) -> Self
        where
            I: IntoIterator<Item = SearchDateRange>
    {
        Self {
            field: field.into(),
            size: None,
            date_ranges: date_ranges.into_iter().collect()
        }
    }
    pub fn size(mut self, size: u32) -> Self {
        self.size = Some(size);
        self
    }
}

impl SearchFacet for DateRangeFacet {}
