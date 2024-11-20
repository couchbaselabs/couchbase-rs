use crate::search::location::Location;
use typed_builder::TypedBuilder;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MatchOperator {
    Or,
    And,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct MatchQuery {
    pub analyzer: Option<String>,
    pub boost: Option<f32>,
    pub field: Option<String>,
    pub fuzziness: Option<u64>,
    #[builder(!default)]
    pub match_input: String,
    pub operator: Option<MatchOperator>,
    pub prefix_length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct MatchPhraseQuery {
    pub analyzer: Option<String>,
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub match_phrase: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct RegexpQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub regexp: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct QueryStringQuery {
    pub boost: Option<f32>,
    #[builder(!default)]
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct NumericRangeQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    pub inclusive_min: Option<bool>,
    pub inclusive_max: Option<bool>,
    pub min: Option<f32>,
    pub max: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DateRangeQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    pub datetime_parser: Option<String>,
    pub end: Option<String>,
    pub inclusive_start: Option<bool>,
    pub inclusive_end: Option<bool>,
    pub start: Option<String>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermRangeQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    pub inclusive_min: Option<bool>,
    pub inclusive_max: Option<bool>,
    pub max: Option<String>,
    pub min: Option<String>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ConjunctionQuery {
    pub boost: Option<f32>,
    #[builder(!default)]
    pub conjuncts: Vec<Query>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DisjunctionQuery {
    pub boost: Option<f32>,
    #[builder(!default)]
    pub disjuncts: Vec<Query>,
    #[builder(!default)]
    pub min: u32,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct BooleanQuery {
    pub boost: Option<f32>,
    pub must: Option<ConjunctionQuery>,
    pub must_not: Option<DisjunctionQuery>,
    pub should: Option<DisjunctionQuery>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct WildcardQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub wildcard: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DocIDQuery {
    pub boost: Option<f32>,
    #[builder(!default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct BooleanFieldQuery {
    #[builder(!default)]
    pub bool_value: bool,
    pub boost: Option<f32>,
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    pub fuzziness: Option<u32>,
    pub prefix_length: Option<u32>,
    #[builder(!default)]
    pub term: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct PhraseQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct PrefixQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct MatchAllQuery {}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct MatchNoneQuery {}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoDistanceQuery {
    #[builder(!default)]
    pub distance: String,
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub location: Location,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoBoundingBoxQuery {
    #[builder(!default)]
    pub bottom_right: Location,
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub top_left: Location,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct GeoPolygonQuery {
    pub boost: Option<f32>,
    pub field: Option<String>,
    #[builder(!default)]
    pub polygon_points: Vec<Location>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Match(MatchQuery),
    MatchPhrase(MatchPhraseQuery),
    Regexp(RegexpQuery),
    #[allow(clippy::enum_variant_names)]
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

impl From<MatchOperator> for couchbase_core::searchx::queries::MatchOperator {
    fn from(operator: MatchOperator) -> Self {
        match operator {
            MatchOperator::Or => couchbase_core::searchx::queries::MatchOperator::Or,
            MatchOperator::And => couchbase_core::searchx::queries::MatchOperator::And,
        }
    }
}

impl From<MatchOperator> for Option<couchbase_core::searchx::queries::MatchOperator> {
    fn from(operator: MatchOperator) -> Self {
        match operator {
            MatchOperator::Or => Some(couchbase_core::searchx::queries::MatchOperator::Or),
            MatchOperator::And => Some(couchbase_core::searchx::queries::MatchOperator::And),
        }
    }
}

impl From<MatchQuery> for couchbase_core::searchx::queries::MatchQuery {
    fn from(query: MatchQuery) -> Self {
        couchbase_core::searchx::queries::MatchQuery::builder()
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .match_input(query.match_input)
            .operator(query.operator.map(|o| o.into()))
            .prefix_length(query.prefix_length)
            .build()
    }
}

impl From<MatchPhraseQuery> for couchbase_core::searchx::queries::MatchPhraseQuery {
    fn from(query: MatchPhraseQuery) -> Self {
        couchbase_core::searchx::queries::MatchPhraseQuery::builder()
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
            .match_phrase(query.match_phrase)
            .build()
    }
}

impl From<RegexpQuery> for couchbase_core::searchx::queries::RegexpQuery {
    fn from(query: RegexpQuery) -> Self {
        couchbase_core::searchx::queries::RegexpQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .regexp(query.regexp)
            .build()
    }
}

impl From<QueryStringQuery> for couchbase_core::searchx::queries::QueryStringQuery {
    fn from(query: QueryStringQuery) -> Self {
        couchbase_core::searchx::queries::QueryStringQuery::builder()
            .boost(query.boost)
            .query(query.query)
            .build()
    }
}

impl From<NumericRangeQuery> for couchbase_core::searchx::queries::NumericRangeQuery {
    fn from(query: NumericRangeQuery) -> Self {
        couchbase_core::searchx::queries::NumericRangeQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .min(query.min)
            .max(query.max)
            .build()
    }
}

impl From<DateRangeQuery> for couchbase_core::searchx::queries::DateRangeQuery {
    fn from(query: DateRangeQuery) -> Self {
        couchbase_core::searchx::queries::DateRangeQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .datetime_parser(query.datetime_parser)
            .end(query.end)
            .inclusive_start(query.inclusive_start)
            .inclusive_end(query.inclusive_end)
            .start(query.start)
            .build()
    }
}

impl From<TermRangeQuery> for couchbase_core::searchx::queries::TermRangeQuery {
    fn from(query: TermRangeQuery) -> Self {
        couchbase_core::searchx::queries::TermRangeQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .max(query.max)
            .min(query.min)
            .build()
    }
}

impl From<ConjunctionQuery> for couchbase_core::searchx::queries::ConjunctionQuery {
    fn from(query: ConjunctionQuery) -> Self {
        couchbase_core::searchx::queries::ConjunctionQuery::builder()
            .boost(query.boost)
            .conjuncts(
                query
                    .conjuncts
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
            )
            .build()
    }
}

impl From<DisjunctionQuery> for couchbase_core::searchx::queries::DisjunctionQuery {
    fn from(query: DisjunctionQuery) -> Self {
        couchbase_core::searchx::queries::DisjunctionQuery::builder()
            .boost(query.boost)
            .disjuncts(
                query
                    .disjuncts
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
            )
            .min(query.min)
            .build()
    }
}

impl From<BooleanQuery> for couchbase_core::searchx::queries::BooleanQuery {
    fn from(query: BooleanQuery) -> Self {
        couchbase_core::searchx::queries::BooleanQuery::builder()
            .boost(query.boost)
            .must(query.must.map(Into::into))
            .must_not(query.must_not.map(Into::into))
            .should(query.should.map(Into::into))
            .build()
    }
}

impl From<WildcardQuery> for couchbase_core::searchx::queries::WildcardQuery {
    fn from(query: WildcardQuery) -> Self {
        couchbase_core::searchx::queries::WildcardQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .wildcard(query.wildcard)
            .build()
    }
}

impl From<DocIDQuery> for couchbase_core::searchx::queries::DocIDQuery {
    fn from(query: DocIDQuery) -> Self {
        couchbase_core::searchx::queries::DocIDQuery::builder()
            .boost(query.boost)
            .ids(query.ids)
            .build()
    }
}

impl From<BooleanFieldQuery> for couchbase_core::searchx::queries::BooleanFieldQuery {
    fn from(query: BooleanFieldQuery) -> Self {
        couchbase_core::searchx::queries::BooleanFieldQuery::builder()
            .bool_value(query.bool_value)
            .boost(query.boost)
            .field(query.field)
            .build()
    }
}

impl From<TermQuery> for couchbase_core::searchx::queries::TermQuery {
    fn from(query: TermQuery) -> Self {
        couchbase_core::searchx::queries::TermQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .prefix_length(query.prefix_length)
            .term(query.term)
            .build()
    }
}

impl From<PhraseQuery> for couchbase_core::searchx::queries::PhraseQuery {
    fn from(query: PhraseQuery) -> Self {
        couchbase_core::searchx::queries::PhraseQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .terms(query.terms)
            .build()
    }
}

impl From<PrefixQuery> for couchbase_core::searchx::queries::PrefixQuery {
    fn from(query: PrefixQuery) -> Self {
        couchbase_core::searchx::queries::PrefixQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .prefix(query.prefix)
            .build()
    }
}

impl From<MatchAllQuery> for couchbase_core::searchx::queries::MatchAllQuery {
    fn from(_: MatchAllQuery) -> Self {
        couchbase_core::searchx::queries::MatchAllQuery::builder().build()
    }
}

impl From<MatchNoneQuery> for couchbase_core::searchx::queries::MatchNoneQuery {
    fn from(_: MatchNoneQuery) -> Self {
        couchbase_core::searchx::queries::MatchNoneQuery::builder().build()
    }
}

impl From<GeoDistanceQuery> for couchbase_core::searchx::queries::GeoDistanceQuery {
    fn from(query: GeoDistanceQuery) -> Self {
        couchbase_core::searchx::queries::GeoDistanceQuery::builder()
            .distance(query.distance)
            .boost(query.boost)
            .field(query.field)
            .location(query.location)
            .build()
    }
}

impl From<GeoBoundingBoxQuery> for couchbase_core::searchx::queries::GeoBoundingBoxQuery {
    fn from(query: GeoBoundingBoxQuery) -> Self {
        couchbase_core::searchx::queries::GeoBoundingBoxQuery::builder()
            .bottom_right(query.bottom_right)
            .boost(query.boost)
            .field(query.field)
            .top_left(query.top_left)
            .build()
    }
}

impl From<GeoPolygonQuery> for couchbase_core::searchx::queries::GeoPolygonQuery {
    fn from(query: GeoPolygonQuery) -> Self {
        couchbase_core::searchx::queries::GeoPolygonQuery::builder()
            .boost(query.boost)
            .field(query.field)
            .polygon_points(
                query
                    .polygon_points
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<couchbase_core::searchx::query_options::Location>>(),
            )
            .build()
    }
}

impl From<Query> for couchbase_core::searchx::queries::Query {
    fn from(query: Query) -> Self {
        match query {
            Query::Match(q) => couchbase_core::searchx::queries::Query::Match(q.into()),
            Query::MatchPhrase(q) => couchbase_core::searchx::queries::Query::MatchPhrase(q.into()),
            Query::Regexp(q) => couchbase_core::searchx::queries::Query::Regexp(q.into()),
            Query::QueryString(q) => couchbase_core::searchx::queries::Query::QueryString(q.into()),
            Query::NumericRange(q) => {
                couchbase_core::searchx::queries::Query::NumericRange(q.into())
            }
            Query::DateRange(q) => couchbase_core::searchx::queries::Query::DateRange(q.into()),
            Query::TermRange(q) => couchbase_core::searchx::queries::Query::TermRange(q.into()),
            Query::Conjunction(q) => couchbase_core::searchx::queries::Query::Conjunction(q.into()),
            Query::Disjunction(q) => couchbase_core::searchx::queries::Query::Disjunction(q.into()),
            Query::Boolean(q) => couchbase_core::searchx::queries::Query::Boolean(q.into()),
            Query::Wildcard(q) => couchbase_core::searchx::queries::Query::Wildcard(q.into()),
            Query::DocID(q) => couchbase_core::searchx::queries::Query::DocID(q.into()),
            Query::BooleanField(q) => {
                couchbase_core::searchx::queries::Query::BooleanField(q.into())
            }
            Query::Term(q) => couchbase_core::searchx::queries::Query::Term(q.into()),
            Query::Phrase(q) => couchbase_core::searchx::queries::Query::Phrase(q.into()),
            Query::Prefix(q) => couchbase_core::searchx::queries::Query::Prefix(q.into()),
            Query::MatchAll(q) => couchbase_core::searchx::queries::Query::MatchAll(q.into()),
            Query::MatchNone(q) => couchbase_core::searchx::queries::Query::MatchNone(q.into()),
            Query::GeoDistance(q) => couchbase_core::searchx::queries::Query::GeoDistance(q.into()),
            Query::GeoBoundingBox(q) => {
                couchbase_core::searchx::queries::Query::GeoBoundingBox(q.into())
            }
            Query::GeoPolygon(q) => couchbase_core::searchx::queries::Query::GeoPolygon(q.into()),
        }
    }
}
