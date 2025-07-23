use crate::search::location::Location;
use std::fmt::Display;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MatchOperator {
    Or,
    And,
}

impl TryFrom<&str> for MatchOperator {
    type Error = crate::error::Error;

    fn try_from(operator: &str) -> Result<Self, Self::Error> {
        match operator {
            "or" => Ok(MatchOperator::Or),
            "and" => Ok(MatchOperator::And),
            "OR" => Ok(MatchOperator::Or),
            "AND" => Ok(MatchOperator::And),
            _ => Err(crate::error::Error::invalid_argument(
                "operator",
                "invalid match operator",
            )),
        }
    }
}

impl TryFrom<String> for MatchOperator {
    type Error = crate::error::Error;

    fn try_from(operator: String) -> Result<Self, Self::Error> {
        Self::try_from(operator.as_str())
    }
}

impl Display for MatchOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchOperator::Or => write!(f, "or"),
            MatchOperator::And => write!(f, "and"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchQuery {
    pub(crate) analyzer: Option<String>,
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) fuzziness: Option<u64>,
    pub(crate) match_input: String,
    pub(crate) operator: Option<MatchOperator>,
    pub(crate) prefix_length: Option<u64>,
}

impl MatchQuery {
    pub fn new(match_input: impl Into<String>) -> Self {
        Self {
            analyzer: None,
            boost: None,
            field: None,
            fuzziness: None,
            match_input: match_input.into(),
            operator: None,
            prefix_length: None,
        }
    }

    pub fn analyzer(mut self, analyzer: impl Into<String>) -> Self {
        self.analyzer = Some(analyzer.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    pub fn fuzziness(mut self, fuzziness: u64) -> Self {
        self.fuzziness = Some(fuzziness);
        self
    }

    pub fn operator(mut self, operator: impl Into<MatchOperator>) -> Self {
        self.operator = Some(operator.into());
        self
    }

    pub fn prefix_length(mut self, prefix_length: u64) -> Self {
        self.prefix_length = Some(prefix_length);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchPhraseQuery {
    pub(crate) analyzer: Option<String>,
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) match_phrase: String,
}

impl MatchPhraseQuery {
    pub fn new(match_phrase: impl Into<String>) -> Self {
        Self {
            analyzer: None,
            boost: None,
            field: None,
            match_phrase: match_phrase.into(),
        }
    }

    pub fn analyzer(mut self, analyzer: impl Into<String>) -> Self {
        self.analyzer = Some(analyzer.into());
        self
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RegexpQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) regexp: String,
}

impl RegexpQuery {
    pub fn new(regexp: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            regexp: regexp.into(),
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct QueryStringQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) query: String,
}

impl QueryStringQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            boost: None,
            query: query.into(),
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRangeQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) inclusive_min: Option<bool>,
    pub(crate) inclusive_max: Option<bool>,
    pub(crate) min: Option<f32>,
    pub(crate) max: Option<f32>,
}

impl NumericRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    pub fn inclusive_min(mut self, min: f32, inclusive_min: bool) -> Self {
        self.min = Some(min);
        self.inclusive_min = Some(inclusive_min);
        self
    }

    pub fn inclusive_max(mut self, max: f32, inclusive_max: bool) -> Self {
        self.max = Some(max);
        self.inclusive_max = Some(inclusive_max);
        self
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

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRangeQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) datetime_parser: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) inclusive_start: Option<bool>,
    pub(crate) inclusive_end: Option<bool>,
    pub(crate) start: Option<String>,
}

impl DateRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    pub fn datetime_parser(mut self, datetime_parser: impl Into<String>) -> Self {
        self.datetime_parser = Some(datetime_parser.into());
        self
    }

    pub fn inclusive_start(mut self, start: impl Into<String>, inclusive_start: bool) -> Self {
        self.start = Some(start.into());
        self.inclusive_start = Some(inclusive_start);
        self
    }

    pub fn inclusive_end(mut self, end: impl Into<String>, inclusive_end: bool) -> Self {
        self.end = Some(end.into());
        self.inclusive_end = Some(inclusive_end);
        self
    }

    pub fn end(mut self, end: impl Into<String>) -> Self {
        self.end = Some(end.into());
        self
    }

    pub fn start(mut self, start: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct TermRangeQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) inclusive_min: Option<bool>,
    pub(crate) inclusive_max: Option<bool>,
    pub(crate) max: Option<String>,
    pub(crate) min: Option<String>,
}

impl TermRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    pub fn inclusive_min(mut self, min: impl Into<String>, inclusive_min: bool) -> Self {
        self.min = Some(min.into());
        self.inclusive_min = Some(inclusive_min);
        self
    }

    pub fn inclusive_max(mut self, max: impl Into<String>, inclusive_max: bool) -> Self {
        self.max = Some(max.into());
        self.inclusive_max = Some(inclusive_max);
        self
    }

    pub fn max(mut self, max: impl Into<String>) -> Self {
        self.max = Some(max.into());
        self
    }

    pub fn min(mut self, min: impl Into<String>) -> Self {
        self.min = Some(min.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConjunctionQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) conjuncts: Vec<Query>,
}

impl ConjunctionQuery {
    pub fn new(conjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            conjuncts,
        }
    }

    pub fn and(mut self, query: Query) -> Self {
        self.conjuncts.push(query);
        self
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DisjunctionQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) disjuncts: Vec<Query>,
    pub(crate) min: Option<u32>,
}

impl DisjunctionQuery {
    pub fn new(disjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            disjuncts,
            min: None,
        }
    }

    pub fn or(mut self, query: Query) -> Self {
        self.disjuncts.push(query);
        self
    }

    pub fn min(mut self, min: u32) -> Self {
        self.min = Some(min);
        self
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct BooleanQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) must: Option<ConjunctionQuery>,
    pub(crate) must_not: Option<DisjunctionQuery>,
    pub(crate) should: Option<DisjunctionQuery>,
}

impl BooleanQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn must(mut self, must: ConjunctionQuery) -> Self {
        self.must = Some(must);
        self
    }

    pub fn must_not(mut self, must_not: DisjunctionQuery) -> Self {
        self.must_not = Some(must_not);
        self
    }

    pub fn should(mut self, should: DisjunctionQuery) -> Self {
        self.should = Some(should);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WildcardQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) wildcard: String,
}

impl WildcardQuery {
    pub fn new(wildcard: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            wildcard: wildcard.into(),
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DocIDQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) ids: Vec<String>,
}

impl DocIDQuery {
    pub fn new(ids: Vec<String>) -> Self {
        Self { boost: None, ids }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BooleanFieldQuery {
    pub(crate) bool_value: bool,
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
}

impl BooleanFieldQuery {
    pub fn new(bool_value: bool) -> Self {
        Self {
            bool_value,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TermQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) fuzziness: Option<u32>,
    pub(crate) prefix_length: Option<u32>,
    pub(crate) term: String,
}

impl TermQuery {
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            fuzziness: None,
            prefix_length: None,
            term: term.into(),
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    pub fn fuzziness(mut self, fuzziness: u32) -> Self {
        self.fuzziness = Some(fuzziness);
        self
    }

    pub fn prefix_length(mut self, prefix_length: u32) -> Self {
        self.prefix_length = Some(prefix_length);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PhraseQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) terms: Vec<String>,
}

impl PhraseQuery {
    pub fn new(terms: Vec<String>) -> Self {
        Self {
            boost: None,
            field: None,
            terms,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PrefixQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) prefix: String,
}

impl PrefixQuery {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            prefix: prefix.into(),
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchAllQuery {}

impl MatchAllQuery {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchNoneQuery {}

impl MatchNoneQuery {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoDistanceQuery {
    pub(crate) distance: String,
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) location: Location,
}

impl GeoDistanceQuery {
    pub fn new(distance: impl Into<String>, location: Location) -> Self {
        Self {
            distance: distance.into(),
            boost: None,
            field: None,
            location,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoBoundingBoxQuery {
    pub(crate) bottom_right: Location,
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) top_left: Location,
}

impl GeoBoundingBoxQuery {
    pub fn new(bottom_right: Location, top_left: Location) -> Self {
        Self {
            bottom_right,
            boost: None,
            field: None,
            top_left,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoPolygonQuery {
    pub(crate) boost: Option<f32>,
    pub(crate) field: Option<String>,
    pub(crate) polygon_points: Vec<Location>,
}

impl GeoPolygonQuery {
    pub fn new(polygon_points: Vec<Location>) -> Self {
        Self {
            boost: None,
            field: None,
            polygon_points,
        }
    }

    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
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
        couchbase_core::searchx::queries::MatchQuery::new(query.match_input)
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .operator(query.operator.map(|o| o.into()))
            .prefix_length(query.prefix_length)
    }
}

impl From<MatchPhraseQuery> for couchbase_core::searchx::queries::MatchPhraseQuery {
    fn from(query: MatchPhraseQuery) -> Self {
        couchbase_core::searchx::queries::MatchPhraseQuery::new(query.match_phrase)
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<RegexpQuery> for couchbase_core::searchx::queries::RegexpQuery {
    fn from(query: RegexpQuery) -> Self {
        couchbase_core::searchx::queries::RegexpQuery::new(query.regexp)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<QueryStringQuery> for couchbase_core::searchx::queries::QueryStringQuery {
    fn from(query: QueryStringQuery) -> Self {
        couchbase_core::searchx::queries::QueryStringQuery::new(query.query).boost(query.boost)
    }
}

impl From<NumericRangeQuery> for couchbase_core::searchx::queries::NumericRangeQuery {
    fn from(query: NumericRangeQuery) -> Self {
        couchbase_core::searchx::queries::NumericRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .min(query.min)
            .max(query.max)
    }
}

impl From<DateRangeQuery> for couchbase_core::searchx::queries::DateRangeQuery {
    fn from(query: DateRangeQuery) -> Self {
        couchbase_core::searchx::queries::DateRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .datetime_parser(query.datetime_parser)
            .end(query.end)
            .inclusive_start(query.inclusive_start)
            .inclusive_end(query.inclusive_end)
            .start(query.start)
    }
}

impl From<TermRangeQuery> for couchbase_core::searchx::queries::TermRangeQuery {
    fn from(query: TermRangeQuery) -> Self {
        couchbase_core::searchx::queries::TermRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .max(query.max)
            .min(query.min)
    }
}

impl From<ConjunctionQuery> for couchbase_core::searchx::queries::ConjunctionQuery {
    fn from(query: ConjunctionQuery) -> Self {
        couchbase_core::searchx::queries::ConjunctionQuery::new(
            query
                .conjuncts
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
        )
        .boost(query.boost)
    }
}

impl From<DisjunctionQuery> for couchbase_core::searchx::queries::DisjunctionQuery {
    fn from(query: DisjunctionQuery) -> Self {
        couchbase_core::searchx::queries::DisjunctionQuery::new(
            query
                .disjuncts
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
        )
        .boost(query.boost)
        .min(query.min)
    }
}

impl From<BooleanQuery> for couchbase_core::searchx::queries::BooleanQuery {
    fn from(query: BooleanQuery) -> Self {
        couchbase_core::searchx::queries::BooleanQuery::new()
            .boost(query.boost)
            .must(query.must.map(Into::into))
            .must_not(query.must_not.map(Into::into))
            .should(query.should.map(Into::into))
    }
}

impl From<WildcardQuery> for couchbase_core::searchx::queries::WildcardQuery {
    fn from(query: WildcardQuery) -> Self {
        couchbase_core::searchx::queries::WildcardQuery::new(query.wildcard)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<DocIDQuery> for couchbase_core::searchx::queries::DocIDQuery {
    fn from(query: DocIDQuery) -> Self {
        couchbase_core::searchx::queries::DocIDQuery::new(query.ids).boost(query.boost)
    }
}

impl From<BooleanFieldQuery> for couchbase_core::searchx::queries::BooleanFieldQuery {
    fn from(query: BooleanFieldQuery) -> Self {
        couchbase_core::searchx::queries::BooleanFieldQuery::new(query.bool_value)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<TermQuery> for couchbase_core::searchx::queries::TermQuery {
    fn from(query: TermQuery) -> Self {
        couchbase_core::searchx::queries::TermQuery::new(query.term)
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .prefix_length(query.prefix_length)
    }
}

impl From<PhraseQuery> for couchbase_core::searchx::queries::PhraseQuery {
    fn from(query: PhraseQuery) -> Self {
        couchbase_core::searchx::queries::PhraseQuery::new(query.terms)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<PrefixQuery> for couchbase_core::searchx::queries::PrefixQuery {
    fn from(query: PrefixQuery) -> Self {
        couchbase_core::searchx::queries::PrefixQuery::new(query.prefix)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<MatchAllQuery> for couchbase_core::searchx::queries::MatchAllQuery {
    fn from(_: MatchAllQuery) -> Self {
        couchbase_core::searchx::queries::MatchAllQuery::new()
    }
}

impl From<MatchNoneQuery> for couchbase_core::searchx::queries::MatchNoneQuery {
    fn from(_: MatchNoneQuery) -> Self {
        couchbase_core::searchx::queries::MatchNoneQuery::new()
    }
}

impl From<GeoDistanceQuery> for couchbase_core::searchx::queries::GeoDistanceQuery {
    fn from(query: GeoDistanceQuery) -> Self {
        couchbase_core::searchx::queries::GeoDistanceQuery::new(
            query.distance,
            query.location.into(),
        )
        .boost(query.boost)
        .field(query.field)
    }
}

impl From<GeoBoundingBoxQuery> for couchbase_core::searchx::queries::GeoBoundingBoxQuery {
    fn from(query: GeoBoundingBoxQuery) -> Self {
        couchbase_core::searchx::queries::GeoBoundingBoxQuery::new(
            query.top_left,
            query.bottom_right,
        )
        .boost(query.boost)
        .field(query.field)
    }
}

impl From<GeoPolygonQuery> for couchbase_core::searchx::queries::GeoPolygonQuery {
    fn from(query: GeoPolygonQuery) -> Self {
        couchbase_core::searchx::queries::GeoPolygonQuery::new(
            query
                .polygon_points
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::query_options::Location>>(),
        )
        .boost(query.boost)
        .field(query.field)
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
