use crate::searchx::query_options::Location;
use serde::Serialize;
use typed_builder::TypedBuilder;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MatchOperator {
    Or,
    And,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct MatchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzziness: Option<u64>,
    #[builder(!default)]
    #[serde(rename = "match")]
    pub match_input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<MatchOperator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct MatchPhraseQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub match_phrase: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct RegexpQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub regexp: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct QueryStringQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[builder(!default)]
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct NumericRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_min: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_max: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DateRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datetime_parser: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_start: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_end: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub inclusive_min: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_max: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ConjunctionQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[builder(!default)]
    pub conjuncts: Vec<Query>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DisjunctionQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[builder(!default)]
    pub disjuncts: Vec<Query>,
    #[builder(!default)]
    pub min: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct BooleanQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    pub must: Option<ConjunctionQuery>,
    pub must_not: Option<DisjunctionQuery>,
    pub should: Option<DisjunctionQuery>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct WildcardQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub wildcard: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DocIDQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[builder(!default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct BooleanFieldQuery {
    #[builder(!default)]
    #[serde(rename = "bool")]
    pub bool_value: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzziness: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u32>,
    #[builder(!default)]
    pub term: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct PhraseQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct PrefixQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[non_exhaustive]
pub struct MatchAllQuery {}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[non_exhaustive]
pub struct MatchNoneQuery {}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoDistanceQuery {
    #[builder(!default)]
    pub distance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub location: Location,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoBoundingBoxQuery {
    #[builder(!default)]
    pub bottom_right: Location,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub top_left: Location,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoPolygonQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[builder(!default)]
    pub polygon_points: Vec<Location>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Query {
    Match(MatchQuery),
    MatchPhrase(MatchPhraseQuery),
    Regexp(RegexpQuery),
    QueryString(QueryStringQuery),
    NumericRange(NumericRangeQuery),
    DateRange(DateRangeQuery),
    TermRange(TermRangeQuery),
    Conjunction(ConjunctionQuery),
    Disjunction(DisjunctionQuery),
    Boolean(BooleanQuery),
    Wildcard(WildcardQuery),
    DocID(DocIDQuery),
    BooleanField(BooleanFieldQuery),
    Term(TermQuery),
    Phrase(PhraseQuery),
    Prefix(PrefixQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
    GeoDistance(GeoDistanceQuery),
    GeoBoundingBox(GeoBoundingBoxQuery),
    GeoPolygon(GeoPolygonQuery),
}
