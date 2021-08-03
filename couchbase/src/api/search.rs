use serde_json::json;
use crate::CouchbaseError;

pub trait SearchQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError>;
}

pub struct QueryStringQuery {
    query: String,
    boost: Option<f32>,
}

impl QueryStringQuery {
    pub fn new(query: String) -> Self {
        Self { query, boost: None }
    }

    pub fn boost(mut self, boost: f32) -> QueryStringQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for QueryStringQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v =json!({
            "query": &self.query.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct MatchQuery {
    match_query: String,
    boost: Option<f32>,
    field: Option<String>,
    analyzer: Option<String>,
    prefix_length: Option<u64>,
    fuzziness: Option<u64>,
}

impl MatchQuery {
    pub fn new(match_query: String) -> Self {
        Self {
            match_query,
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

    pub fn field(mut self, field: String) -> MatchQuery {
        self.field = Some(field);
        self
    }

    pub fn analyzer(mut self, analyzer: String) -> MatchQuery {
        self.analyzer = Some(analyzer);
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
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "match": &self.match_query.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(val) = self.analyzer.clone() {
            v["analyzer"] = json!(val);
        }
        if let Some(val) = self.prefix_length {
            v["prefix_length"] = json!(val);
        }
        if let Some(val) = self.fuzziness {
            v["fuzziness"] = json!(val);
        }
        Ok(v)
    }
}

pub struct MatchPhraseQuery {
    phrase: String,
    boost: Option<f32>,
    field: Option<String>,
    analyzer: Option<String>,
}

impl MatchPhraseQuery {
    pub fn new(phrase: String) -> Self {
        Self {
            phrase,
            boost: None,
            field: None,
            analyzer: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> MatchPhraseQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: String) -> MatchPhraseQuery {
        self.field = Some(field);
        self
    }

    pub fn analyzer(mut self, analyzer: String) -> MatchPhraseQuery {
        self.analyzer = Some(analyzer);
        self
    }
}

impl SearchQuery for MatchPhraseQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "match_phrase": &self.phrase.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(val) = self.analyzer.clone() {
            v["analyzer"] = json!(val);
        }
        Ok(v)
    }
}

pub struct RegexpQuery {
    regexp: String,
    boost: Option<f32>,
    field: Option<String>,
}

impl RegexpQuery {
    pub fn new(regexp: String) -> Self {
        Self {
            regexp,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> RegexpQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: String) -> RegexpQuery {
        self.field = Some(field);
        self
    }
}

impl SearchQuery for RegexpQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "regexp": &self.regexp.clone(),
        });
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        Ok(v)
    }
}

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

    pub fn field(mut self, field: String) -> NumericRangeQuery {
        self.field = Some(field);
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
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({});
        if let Some(val) = self.min {
            v["min"] = json!(val);
        }
        if let Some(val) = self.max {
            v["max"] = json!(val);
        }
        if let Some(val) = self.inclusive_min {
            v["inclusive_min"] = json!(val);
        }
        if let Some(val) = self.inclusive_max {
            v["inclusive_max"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        Ok(v)
    }
}

pub struct DateRangeQuery {
    start: Option<String>,
    end: Option<String>,
    inclusive_start: Option<bool>,
    inclusive_end: Option<bool>,
    parser: Option<String>,
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
            parser: None,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> DateRangeQuery {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: String) -> DateRangeQuery {
        self.field = Some(field);
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

    pub fn datetime_parser(mut self, parser: String) -> DateRangeQuery {
        self.parser = Some(parser);
        self
    }
}

impl SearchQuery for DateRangeQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({});
        if let Some(val) = self.start.clone() {
            v["start"] = json!(val);
        }
        if let Some(val) = self.end.clone() {
            v["end"] = json!(val);
        }
        if let Some(val) = self.inclusive_start {
            v["inclusive_start"] = json!(val);
        }
        if let Some(val) = self.inclusive_end {
            v["inclusive_end"] = json!(val);
        }
        if let Some(val) = self.parser.clone() {
            v["datetime_parser"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
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
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({});
        let mut queries = vec![];
        for query in &self.conjuncts {
            queries.push(query.to_json()?);
        }
        v["conjuncts"] = json!(queries);
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
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
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({});
        let mut queries = vec![];
        for query in &self.disjuncts {
            queries.push(query.to_json()?);
        }
        v["disjuncts"] = json!(queries);
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        if let Some(b) = self.min {
            v["min"] = json!(b);
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
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
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
                payload["min"] = json!(min);
            }
            v["should"] = payload;
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

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

    pub fn field(mut self, field: String) -> WildcardQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> WildcardQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for WildcardQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "wildcard": self.wildcard
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct DocIDQuery {
    doc_ids: Vec<String>,

    field: Option<String>,
    boost: Option<f32>,
}

impl DocIDQuery {
    pub fn new(doc_ids: Vec<String>) -> Self {
        Self {
            doc_ids,
            field: None,
            boost: None,
        }
    }

    pub fn add_doc_ids(mut self, doc_ids: Vec<String>) -> DocIDQuery {
        self.doc_ids.extend(doc_ids);
        self
    }

    pub fn field(mut self, field: String) -> DocIDQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> DocIDQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for DocIDQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "ids": self.doc_ids
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct BooleanFieldQuery {
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

    pub fn field(mut self, field: String) -> BooleanFieldQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> BooleanFieldQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for BooleanFieldQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "bool": self.val
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct TermQuery {
    term: String,

    field: Option<String>,
    fuzziness: Option<u64>,
    prefix_length: Option<u64>,
    boost: Option<f32>,
}

impl TermQuery {
    pub fn new(term: String) -> Self {
        Self {
            term,
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

    pub fn field(mut self, field: String) -> TermQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> TermQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for TermQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "term": self.term.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(val) = self.prefix_length.clone() {
            v["prefix_length"] = json!(val);
        }
        if let Some(val) = self.fuzziness.clone() {
            v["fuzziness"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct PhraseQuery {
    terms: Vec<String>,

    field: Option<String>,
    boost: Option<f32>,
}

impl PhraseQuery {
    pub fn new(terms: Vec<String>) -> Self {
        Self {
            terms,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: String) -> PhraseQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> PhraseQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for PhraseQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "terms": self.terms
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct PrefixQuery {
    prefix: String,

    field: Option<String>,
    boost: Option<f32>,
}

impl PrefixQuery {
    pub fn new(prefix: String) -> Self {
        Self {
            prefix,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: String) -> PrefixQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> PrefixQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for PrefixQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "prefix": self.prefix.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct MatchAllQuery {
}

impl MatchAllQuery {
    pub fn new() -> Self {
        Self {}
    }
}

impl SearchQuery for MatchAllQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        Ok(json!({
            "match_all":  serde_json::Value::Null,
        }))
    }
}

pub struct MatchNoneQuery {
}

impl MatchNoneQuery {
    pub fn new() -> Self {
        Self {}
    }
}

impl SearchQuery for MatchNoneQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        Ok(json!({
            "match_none": serde_json::Value::Null,
        }))
    }
}

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
    pub fn new(term: String) -> Self {
        Self {
            term,
            field: None,
            boost: None,
            min: None,
            max: None,
            inclusive_min: None,
            inclusive_max: None
        }
    }

    pub fn field(mut self, field: String) -> TermRangeQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> TermRangeQuery {
        self.boost = Some(boost);
        self
    }

    pub fn min(mut self, min: String, inclusive: bool) -> TermRangeQuery {
        self.min = Some(min);
        self.inclusive_min = Some(inclusive);
        self
    }

    pub fn max(mut self, max: String, inclusive: bool) -> TermRangeQuery {
        self.max = Some(max);
        self.inclusive_max = Some(inclusive);
        self
    }
}

impl SearchQuery for TermRangeQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "term": self.term.clone()
        });
        if let Some(val) = self.min.clone() {
            v["min"] = json!(val);
        }
        if let Some(val) = self.max.clone() {
            v["max"] = json!(val);
        }
        if let Some(val) = self.inclusive_min {
            v["inclusive_min"] = json!(val);
        }
        if let Some(val) = self.inclusive_max {
            v["inclusive_max"] = json!(val);
        }
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct GeoDistanceQuery {
    location: [f64; 2],
    distance: String,

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoDistanceQuery {
    pub fn new(lon: f64, lat: f64, distance: String) -> Self {
        Self {
            location: [lon, lat],
            distance,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: String) -> GeoDistanceQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoDistanceQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoDistanceQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "location": self.location,
            "distance": self.distance.clone()
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct GeoBoundingBoxQuery {
    top_left: [f64; 2],
    bottom_right: [f64; 2],

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoBoundingBoxQuery {
    pub fn new(top_left_lon: f64, top_left_lat: f64, bottom_right_lon: f64, bottom_right_lat: f64) -> Self {
        Self {
            top_left: [top_left_lon, top_left_lat],
            bottom_right: [bottom_right_lon, bottom_right_lat],
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: String) -> GeoBoundingBoxQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoBoundingBoxQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoBoundingBoxQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut v = json!({
            "top_left": self.top_left,
            "bottom_right": self.bottom_right,
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}

pub struct Coordinate {
    lon: f64,
    lat: f64
}

impl Coordinate {
    pub fn new(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

pub struct GeoPolygonQuery {
    coordinates: Vec<Coordinate>,

    field: Option<String>,
    boost: Option<f32>,
}

impl GeoPolygonQuery {
    pub fn new(coordinates: Vec<Coordinate>) -> Self {
        Self {
            coordinates,
            field: None,
            boost: None,
        }
    }

    pub fn field(mut self, field: String) -> GeoPolygonQuery {
        self.field = Some(field);
        self
    }

    pub fn boost(mut self, boost: f32) -> GeoPolygonQuery {
        self.boost = Some(boost);
        self
    }
}

impl SearchQuery for GeoPolygonQuery {
    fn to_json(&self) -> Result<serde_json::Value, CouchbaseError> {
        let mut points = vec![];
        for coord in &self.coordinates {
            points.push([coord.lon, coord.lat]);
        }
        let mut v = json!({
            "polygon_points": points,
        });
        if let Some(val) = self.field.clone() {
            v["field"] = json!(val);
        }
        if let Some(b) = self.boost {
            v["boost"] = json!(b);
        }
        Ok(v)
    }
}
