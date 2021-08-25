use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::MutationToken;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;

use serde_json::{Value};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration};
use serde_derive::Deserialize;
use std::io::{Error, ErrorKind};
use std::convert::TryFrom;
use std::iter::FromIterator;


#[derive(Debug)]
pub struct QueryResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<QueryMetaData>>,
}

impl QueryResult {
    pub(crate) fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<QueryMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> QueryMetaData {
        self.meta.take().unwrap().await.unwrap()
    }
}

// TODO: add status, signature, profile, warnings

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
    metrics: QueryMetrics,
}

impl QueryMetaData {
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    pub fn request_id(&self) -> &str {
        self.request_id.as_ref()
    }

    pub fn client_context_id(&self) -> &str {
        self.client_context_id.as_ref()
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "sortCount", default)]
    sort_count: usize,
    #[serde(rename = "resultCount")]
    result_count: usize,
    #[serde(rename = "resultSize")]
    result_size: usize,
    #[serde(rename = "mutationCount", default)]
    mutation_count: usize,
    #[serde(rename = "errorCount", default)]
    error_count: usize,
    #[serde(rename = "warningCount", default)]
    warning_count: usize,
}

impl QueryMetrics {
    pub fn elapsed_time(&self) -> Duration {
        match parse_duration::parse(&self.elapsed_time) {
            Ok(d) => d,
            Err(_e) => Duration::from_secs(0),
        }
    }

    pub fn execution_time(&self) -> Duration {
        match parse_duration::parse(&self.execution_time) {
            Ok(d) => d,
            Err(_e) => Duration::from_secs(0),
        }
    }

    pub fn sort_count(&self) -> usize {
        self.sort_count
    }

    pub fn result_count(&self) -> usize {
        self.result_count
    }

    pub fn result_size(&self) -> usize {
        self.result_size
    }

    pub fn mutation_count(&self) -> usize {
        self.mutation_count
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }

    pub fn warning_count(&self) -> usize {
        self.warning_count
    }
}

#[derive(Debug)]
pub struct AnalyticsResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<AnalyticsMetaData>>,
}

impl AnalyticsResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<AnalyticsMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> AnalyticsMetaData {
        self.meta.take().unwrap().await.unwrap()
    }
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchMetaData {
    errors: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchRowLocation {
    field: String,
    term: String,
    position: u32,
    start: u32,
    end: u32,
    array_positions: Option<Vec<u32>>,
}

impl SearchRowLocation {
    pub fn field(&self) -> String {
        self.field.clone()
    }
    pub fn term(&self) -> String {
        self.term.clone()
    }
    pub fn position(&self) -> u32 {
        self.position
    }
    pub fn start(&self) -> u32 {
        self.start
    }
    pub fn end(&self) -> u32 {
        self.end
    }
    pub fn array_positions(&self) -> Option<Vec<u32>> {
        self.array_positions.clone()
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchRowLocations {
    #[serde(flatten)]
    locations: HashMap<String, HashMap<String, Vec<SearchRowLocation>>>,
}

impl SearchRowLocations {
    pub fn get_all(&self) -> Vec<&SearchRowLocation> {
        let mut locations  = vec![];
        for f in &self.locations {
            for t in f.1 {
                locations.extend(t.1.iter());
            }
        }

        locations
    }

    pub fn get(&self, field: impl Into<String>) -> Vec<&SearchRowLocation> {
        match self.locations.get(&field.into()) {
            Some(fl) => {
                let mut locations  = vec![];
                for t in fl {
                    locations.extend(t.1.iter());
                }

                locations
            },
            None => vec![],
        }
    }

    pub fn get_by_term(&self, field: impl Into<String>, term: impl Into<String>) -> Vec<&SearchRowLocation> {
        match self.locations.get(&field.into()) {
            Some(fl) => {
                match fl.get(&term.into()) {
                    Some(tl) => {
                        let mut locations  = vec![];
                        locations.extend(tl);

                        locations
                    },
                    None => Vec::new(),
                }
            },
            None => vec![],
        }
    }

    pub fn fields(&self) -> Vec<String> {
        self.locations.keys().cloned().collect()
    }

    pub fn terms(&self) -> Vec<String> {
        let mut set = HashSet::new();
        for fl in &self.locations {
            for tl in fl.1 {
                set.insert(tl.0.clone());
            }
        }

        Vec::from_iter(set)
    }

    pub fn terms_for(&self, field: impl Into<String>) -> Vec<String> {
        match self.locations.get(&field.into()) {
            Some(fl) => {
                let mut locations  = vec![];
                for t in fl {
                    locations.push(t.0.clone());
                }

                locations
            },
            None => vec![],
        }
    }
}

impl TryFrom<&Value> for SearchRowLocations {
    type Error = CouchbaseError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        if !value.is_object() {
            return Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected top level to be an object")
            });
        }

        let mut locations = HashMap::new();
        for item in value.as_object().unwrap() {
            let field = item.0;
            let terms = item.1;
            let mut field_terms = HashMap::new();

            if !terms.is_object() {
                return Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected field to be an object")
                });
            }

            for term in terms.as_object().unwrap() {
                let term_name = term.0;
                let term_value = term.1;
                let mut term_locations = vec![];

                if !term_value.is_array() {
                    return Err(CouchbaseError::DecodingFailure {
                        ctx: ErrorContext::default(),
                        source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected term to be an array")
                    });
                }

                for location in term_value.as_array().unwrap() {
                    if !location.is_object() {
                        return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected location to be an array")
                        });
                    }

                    let position =  match location.get("pos") {
                        Some(v) => match v.as_u64() {
                            Some(p) => p as u32,
                            None => return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected pos to be a number")
                            }),
                        },
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `pos`")
                        }),
                    };

                    let start =  match location.get("start") {
                        Some(v) => match v.as_u64() {
                            Some(p) => p as u32,
                            None => return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected start to be a number")
                            }),
                        },
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `start`")
                        }),
                    };

                    let end =  match location.get("end") {
                        Some(v) => match v.as_u64() {
                            Some(p) => p as u32,
                            None => return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type, expected end to be a number")
                            }),
                        },
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `end`")
                        }),
                    };

                    let array_positions =  match location.get("array_positions") {
                        Some(v) =>  {
                            match v.as_array() {
                                Some(p) => {
                                    Some(p.into_iter().map(|item| item.as_u64().unwrap() as u32).collect())
                                },
                                None => None,
                            }
                        },
                        None => None,
                    };

                    term_locations.push( SearchRowLocation{
                        field: field.clone(),
                        term: term_name.clone(),
                        position,
                        start,
                        end,
                        array_positions,
                    });
                }
                field_terms.insert(term_name.clone(), term_locations);
            }

            locations.insert(field.clone(), field_terms);
        }

        Ok(Self {
            locations,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchRow {
    index: String,
    id: String,
    score: f32,
    fields: Option<Value>,
    locations: Option<SearchRowLocations>,
    fragments: Option<Value>,

}

impl SearchRow {
    pub fn index(&self) -> &str {
        &self.index
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn score(&self) -> f32 {
        self.score
    }

    pub fn fields<T>(&mut self) -> Option<impl IntoIterator<Item = CouchbaseResult<T>>>
    where
        T: DeserializeOwned
    {
        if self.fields.is_some() {
            return Some(self.fields.take().into_iter().map(
                |v| {
                    match serde_json::from_value(v) {
                        Ok(decoded) => Ok(decoded),
                        Err(e) => Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: e.into(),
                        }),
                    }
                }));
        }

        return None
    }

    pub fn locations(&self) -> Option<&SearchRowLocations> {
        self.locations.as_ref()
    }

    pub fn fragments<T>(&self) -> Option<Result<T, CouchbaseError>>
    where
        T: DeserializeOwned
    {
        let fragments = self.fragments.as_ref()?.clone();
        Some(serde_json::from_value(fragments).map_err(|e|CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        }))
    }
}

#[derive(Debug)]
pub enum SearchFacetResult {
    DateRangeSearchFacetResult(DateRangeSearchFacetResult),
    NumericRangeSearchFacetResult(NumericRangeSearchFacetResult),
    TermSearchFacetResult(TermSearchFacetResult),
}

#[derive(Debug, Deserialize)]
pub struct SearchDateRangeResult {
    name: String,
    start: String,
    end: String,
    count: u64,
}

impl SearchDateRangeResult {
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn start(&self) -> String {
        self.start.clone()
    }
    pub fn end(&self) -> String {
        self.end.clone()
    }
    pub fn count(&self) -> u64 {
        self.count
    }
}

#[derive(Debug, Deserialize)]
pub struct DateRangeSearchFacetResult {
    field: String,
    name: String,
    total: u64,
    missing: u64,
    other: u64,
    date_ranges: Vec<SearchDateRangeResult>
}

impl DateRangeSearchFacetResult {
    pub fn field(&self) -> String {
        self.field.clone()
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn missing(&self) -> u64 {
        self.missing
    }
    pub fn other(&self) -> u64 {
        self.other
    }
    pub fn date_ranges(&self) -> impl Iterator<Item=&SearchDateRangeResult> {
        self.date_ranges.iter()
    }
}

impl TryFrom<(&String, &Value)> for DateRangeSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: DateRangeSearchFacetResult = serde_json::from_value(value.1.clone()).map_err(|e|CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })?;
        facet.name = value.0.clone();
        Ok(facet)
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchNumericRangeResult {
    name: String,
    start: String,
    end: String,
    count: u64,
}

impl SearchNumericRangeResult {
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn start(&self) -> String {
        self.start.clone()
    }
    pub fn end(&self) -> String {
        self.end.clone()
    }
    pub fn count(&self) -> u64 {
        self.count
    }
}

#[derive(Debug, Deserialize)]
pub struct NumericRangeSearchFacetResult {
    field: String,
    name: String,
    total: u64,
    missing: u64,
    other: u64,
    numeric_ranges: Vec<SearchNumericRangeResult>
}

impl TryFrom<(&String, &Value)> for NumericRangeSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: NumericRangeSearchFacetResult = serde_json::from_value(value.1.clone()).map_err(|e|CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })?;
        facet.name = value.0.clone();
        Ok(facet)
    }
}

impl NumericRangeSearchFacetResult {
    pub fn field(&self) -> String {
        self.field.clone()
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn missing(&self) -> u64 {
        self.missing
    }
    pub fn other(&self) -> u64 {
        self.other
    }
    pub fn numeric_ranges(&self) -> impl Iterator<Item=&SearchNumericRangeResult> {
        self.numeric_ranges.iter()
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchTermResult {
    term: String,
    count: u64,
}

impl SearchTermResult {
    pub fn term(&self) -> String {
        self.term.clone()
    }
    pub fn count(&self) -> u64 {
        self.count
    }
}

#[derive(Debug, Deserialize)]
pub struct TermSearchFacetResult {
    field: String,
    #[serde(skip)]
    name: String,
    total: u64,
    missing: u64,
    other: u64,
    #[serde(default)]
    terms: Vec<SearchTermResult>
}

impl TryFrom<(&String, &Value)> for TermSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: TermSearchFacetResult = serde_json::from_value(value.1.clone()).map_err(|e|CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })?;
        facet.name = value.0.clone();
        Ok(facet)
    }
}

impl TermSearchFacetResult {
    pub fn field(&self) -> String {
        self.field.clone()
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn missing(&self) -> u64 {
        self.missing
    }
    pub fn other(&self) -> u64 {
        self.other
    }
    pub fn terms(&self) -> impl Iterator<Item=&SearchTermResult> {
        self.terms.iter()
    }
}

#[derive(Debug)]
pub struct SearchResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<SearchMetaData>>,
    facets: Option<Receiver<Value>>,
}

impl SearchResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<SearchMetaData>, facets: Receiver<Value>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
            facets: Some(facets),
        }
    }

    pub fn rows(&mut self) -> impl Stream<Item = CouchbaseResult<SearchRow>>
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice::<Value>(v.as_slice()) {
                Ok(decoded) => {
                    let index = match decoded.get("index") {
                        Some(i) => i.to_string(),
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `index`")
                        })
                    };
                    let id = match decoded.get("id") {
                        Some(i) => i.to_string(),
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `id`")
                        })
                    };
                    let score = match decoded.get("score") {
                        Some(i) => {
                            match i.as_f64() {
                                Some(f) => f as f32,
                                None => return Err(CouchbaseError::DecodingFailure {
                                    ctx: ErrorContext::default(),
                                    source: Error::new(ErrorKind::InvalidData, "locations in result is not expected type")
                                }),
                            }
                        },
                        None => return Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: Error::new(ErrorKind::InvalidData, "missing field `score`")
                        })
                    };
                    let fields = match decoded.get("fields") {
                        Some(i) => Some(i.clone()),
                        None => None
                    };
                    let locations = match decoded.get("locations") {
                        Some(i) => {
                            Some(SearchRowLocations::try_from(i)?)
                        },
                        None => None
                    };
                    let fragments = match decoded.get("fragments") {
                        Some(i) => {
                            Some(i.clone())
                        },
                        None => None
                    };

                    Ok(SearchRow{
                        index,
                        id,
                        score,
                        fields,
                        locations,
                        fragments
                    })
                },
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> SearchMetaData {
        self.meta.take().unwrap().await.unwrap()
    }

    pub async fn facets(&mut self) -> CouchbaseResult<HashMap<String, SearchFacetResult>>
    {
        match self.facets.take().unwrap().await {
            Ok(val) => {
                if val.is_null() {
                    return Ok(HashMap::new());
                }
                let val = val.as_object().unwrap();
                let mut res: HashMap<String, SearchFacetResult> = HashMap::new();
                for item in val.iter() {
                    if item.1.get("date_ranges").is_some() {
                        res.insert(item.0.clone(), SearchFacetResult::DateRangeSearchFacetResult(DateRangeSearchFacetResult::try_from(item)?));
                    } else if item.1.get("numeric_ranges").is_some() {
                        res.insert(item.0.clone(), SearchFacetResult::NumericRangeSearchFacetResult(NumericRangeSearchFacetResult::try_from(item)?));
                    } else {
                        res.insert(item.0.clone(), SearchFacetResult::TermSearchFacetResult(TermSearchFacetResult::try_from(item)?));
                    }
                }
                Ok(res)
            },
            Err(_e) => Err(CouchbaseError::RequestCanceled {
                ctx: ErrorContext::default(),
            })
        }
    }
}

pub struct GetResult {
    content: Vec<u8>,
    cas: u64,
    flags: u32,
}

impl GetResult {
    pub fn new(content: Vec<u8>, cas: u64, flags: u32) -> Self {
        Self {
            content,
            cas,
            flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(&self.content.as_slice()) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }
}

impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = match std::str::from_utf8(&self.content) {
            Ok(c) => c,
            Err(_e) => "<Not Valid/Printable UTF-8>",
        };
        write!(
            f,
            "GetResult {{ cas: 0x{:x}, flags: 0x{:x}, content: {} }}",
            self.cas, self.flags, content
        )
    }
}

pub struct ExistsResult {
    cas: Option<u64>,
    exists: bool,
}

impl ExistsResult {
    pub fn new(exists: bool, cas: Option<u64>) -> Self {
        Self { exists, cas }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn cas(&self) -> &Option<u64> {
        &self.cas
    }
}

impl fmt::Debug for ExistsResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExistsResult {{ exists: {:?}, cas: {:?} }}",
            self.exists,
            self.cas.map(|c| format!("0x{:x}", c))
        )
    }
}

pub struct MutationResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
}

impl MutationResult {
    pub fn new(cas: u64, mutation_token: Option<MutationToken>) -> Self {
        Self {
            cas,
            mutation_token,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }
}

impl fmt::Debug for MutationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MutationResult {{ cas: 0x{:x}, mutation_token: {:?} }}",
            self.cas, self.mutation_token
        )
    }
}

pub struct CounterResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
    content: u64,
}

impl CounterResult {
    pub fn new(cas: u64, mutation_token: Option<MutationToken>, content: u64) -> Self {
        Self {
            cas,
            mutation_token,
            content,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }

    pub fn content(&self) -> u64 {
        self.content
    }
}

impl fmt::Debug for CounterResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CounterResult {{ cas: 0x{:x}, mutation_token: {:?},  content: {:?}}}",
            self.cas, self.mutation_token, self.content
        )
    }
}

#[derive(Debug)]
pub(crate) struct SubDocField {
    pub status: u32,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct MutateInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl MutateInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

#[derive(Debug)]
pub struct LookupInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl LookupInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self, index: usize) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(
            &self
                .content
                .get(index)
                .expect("index not found")
                .value
                .as_slice(),
        ) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }

    pub fn exists(&self, index: usize) -> bool {
        self.content.get(index).expect("index not found").status == 0
    }
}

#[derive(Debug)]
pub struct GenericManagementResult {
    status: u16,
    payload: Option<Vec<u8>>,
}

impl GenericManagementResult {
    pub fn new(status: u16, payload: Option<Vec<u8>>) -> Self {
        Self { status, payload }
    }

    pub fn payload(&self) -> Option<&Vec<u8>> {
        self.payload.as_ref()
    }

    pub fn http_status(&self) -> u16 {
        self.status
    }
}

#[derive(Debug)]
pub struct PingResult {
    id: String,
    services: HashMap<ServiceType, Vec<EndpointPingReport>>,
}

impl PingResult {
    pub(crate) fn new(id: String, services: HashMap<ServiceType, Vec<EndpointPingReport>>) -> Self {
        Self { id, services }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn endpoints(&self) -> &HashMap<ServiceType, Vec<EndpointPingReport>> {
        &self.services
    }
}

#[derive(Debug)]
pub struct EndpointPingReport {
    local: Option<String>,
    remote: Option<String>,
    status: PingState,
    error: Option<String>,
    latency: Duration,
    scope: Option<String>,
    id: String,
    typ: ServiceType,
}

impl EndpointPingReport {
    pub(crate) fn new(
        local: Option<String>,
        remote: Option<String>,
        status: PingState,
        error: Option<String>,
        latency: Duration,
        scope: Option<String>,
        id: String,
        typ: ServiceType,
    ) -> Self {
        Self {
            local,
            remote,
            status,
            error,
            latency,
            scope,
            id,
            typ,
        }
    }

    pub fn service_type(&self) -> ServiceType {
        self.typ.clone()
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn local(&self) -> Option<String> {
        self.local.clone()
    }

    pub fn remote(&self) -> Option<String> {
        self.remote.clone()
    }

    pub fn state(&self) -> PingState {
        self.status.clone()
    }

    pub fn error(&self) -> Option<String> {
        self.error.clone()
    }

    pub fn namespace(&self) -> Option<String> {
        self.scope.clone()
    }

    pub fn latency(&self) -> Duration {
        self.latency
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum ServiceType {
    Management,
    KeyValue,
    Views,
    Query,
    Search,
    Analytics,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum PingState {
    OK,
    Timeout,
    Error,
    Invalid,
}

impl fmt::Display for PingState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Deserialize)]
pub struct ViewMetaData {
    total_rows: u64,
    debug: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ViewRow {
    pub(crate) id: Option<String>,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl ViewRow {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn key<T>(&self) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(self.key.as_slice()).map_err(|e| CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })
    }

    pub fn value<T>(&self) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(self.value.as_slice()).map_err(|e| CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })
    }
}

#[derive(Debug)]
pub struct ViewResult {
    rows: Option<UnboundedReceiver<ViewRow>>,
    meta: Option<Receiver<ViewMetaData>>,
}

impl ViewResult {
    pub fn new(rows: UnboundedReceiver<ViewRow>, meta: Receiver<ViewMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows(&mut self) -> impl Stream<Item = CouchbaseResult<ViewRow>> {
        self.rows
            .take()
            .expect("Can not consume rows twice!")
            .map(|v| Ok(v))
        // .map(
        // |v| match serde_json::from_slice(v.as_slice()) {
        //     Ok(decoded) => Ok(decoded),
        //     Err(e) => Err(CouchbaseError::DecodingFailure {
        //         ctx: ErrorContext::default(),
        //         source: e.into(),
        //     }),
        // },
        // )
    }

    pub async fn meta_data(&mut self) -> ViewMetaData {
        self.meta.take().unwrap().await.unwrap()
    }
}
