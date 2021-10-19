use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io::{Error, ErrorKind};
use std::iter::FromIterator;

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
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn term(&self) -> &str {
        &self.term
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
    pub fn array_positions(&self) -> Option<&Vec<u32>> {
        self.array_positions.as_ref()
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchRowLocations {
    #[serde(flatten)]
    locations: HashMap<String, HashMap<String, Vec<SearchRowLocation>>>,
}

impl SearchRowLocations {
    pub fn get_all(&self) -> Vec<&SearchRowLocation> {
        let mut locations = vec![];
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
                let mut locations = vec![];
                for t in fl {
                    locations.extend(t.1.iter());
                }

                locations
            }
            None => vec![],
        }
    }

    pub fn get_by_term(
        &self,
        field: impl Into<String>,
        term: impl Into<String>,
    ) -> Vec<&SearchRowLocation> {
        match self.locations.get(&field.into()) {
            Some(fl) => match fl.get(&term.into()) {
                Some(tl) => {
                    let mut locations = vec![];
                    locations.extend(tl);

                    locations
                }
                None => Vec::new(),
            },
            None => vec![],
        }
    }

    pub fn fields(&self) -> Vec<&String> {
        self.locations.keys().collect()
    }

    pub fn terms(&self) -> Vec<&String> {
        let mut set = HashSet::new();
        for fl in &self.locations {
            for tl in fl.1 {
                set.insert(tl.0);
            }
        }

        Vec::from_iter(set)
    }

    pub fn terms_for(&self, field: impl Into<String>) -> Vec<&String> {
        match self.locations.get(&field.into()) {
            Some(fl) => {
                let mut locations = vec![];
                for t in fl {
                    locations.push(t.0);
                }

                locations
            }
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
                source: Error::new(
                    ErrorKind::InvalidData,
                    "locations in result is not expected type, expected top level to be an object",
                ),
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
                    source: Error::new(
                        ErrorKind::InvalidData,
                        "locations in result is not expected type, expected field to be an object",
                    ),
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

                    let array_positions = match location.get("array_positions") {
                        Some(v) => v
                            .as_array()
                            .map(|p| p.iter().map(|item| item.as_u64().unwrap() as u32).collect()),
                        None => None,
                    };

                    term_locations.push(SearchRowLocation {
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

        Ok(Self { locations })
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
        T: DeserializeOwned,
    {
        if self.fields.is_some() {
            return Some(
                self.fields
                    .take()
                    .into_iter()
                    .map(|v| match serde_json::from_value(v) {
                        Ok(decoded) => Ok(decoded),
                        Err(e) => Err(CouchbaseError::DecodingFailure {
                            ctx: ErrorContext::default(),
                            source: e.into(),
                        }),
                    }),
            );
        }

        None
    }

    pub fn locations(&self) -> Option<&SearchRowLocations> {
        self.locations.as_ref()
    }

    pub fn fragments<T>(&self) -> Option<CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        let fragments = self.fragments.as_ref()?.clone();
        Some(
            serde_json::from_value(fragments).map_err(|e| CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        )
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
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn start(&self) -> &str {
        &self.start
    }
    pub fn end(&self) -> &str {
        &self.end
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
    date_ranges: Vec<SearchDateRangeResult>,
}

impl DateRangeSearchFacetResult {
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn date_ranges(&self) -> impl IntoIterator<Item = &SearchDateRangeResult> {
        self.date_ranges.iter()
    }
}

impl TryFrom<(&String, &Value)> for DateRangeSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: DateRangeSearchFacetResult = serde_json::from_value(value.1.clone())
            .map_err(|e| CouchbaseError::DecodingFailure {
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
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn start(&self) -> &str {
        &self.start
    }
    pub fn end(&self) -> &str {
        &self.end
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
    numeric_ranges: Vec<SearchNumericRangeResult>,
}

impl TryFrom<(&String, &Value)> for NumericRangeSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: NumericRangeSearchFacetResult = serde_json::from_value(value.1.clone())
            .map_err(|e| CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            })?;
        facet.name = value.0.clone();
        Ok(facet)
    }
}

impl NumericRangeSearchFacetResult {
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn numeric_ranges(&self) -> impl IntoIterator<Item = &SearchNumericRangeResult> {
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
    terms: Vec<SearchTermResult>,
}

impl TryFrom<(&String, &Value)> for TermSearchFacetResult {
    type Error = CouchbaseError;

    fn try_from(value: (&String, &Value)) -> Result<Self, Self::Error> {
        let mut facet: TermSearchFacetResult =
            serde_json::from_value(value.1.clone()).map_err(|e| {
                CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }
            })?;
        facet.name = value.0.clone();
        Ok(facet)
    }
}

impl TermSearchFacetResult {
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn terms(&self) -> impl Iterator<Item = &SearchTermResult> {
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
    pub fn new(
        rows: UnboundedReceiver<Vec<u8>>,
        meta: Receiver<SearchMetaData>,
        facets: Receiver<Value>,
    ) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
            facets: Some(facets),
        }
    }

    pub fn rows(&mut self) -> impl Stream<Item = CouchbaseResult<SearchRow>> {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice::<Value>(v.as_slice()) {
                Ok(decoded) => {
                    let index = match decoded.get("index") {
                        Some(i) => i.to_string(),
                        None => {
                            return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "missing field `index`"),
                            })
                        }
                    };
                    let id = match decoded.get("id") {
                        Some(i) => i.to_string(),
                        None => {
                            return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "missing field `id`"),
                            })
                        }
                    };
                    let score = match decoded.get("score") {
                        Some(i) => match i.as_f64() {
                            Some(f) => f as f32,
                            None => {
                                return Err(CouchbaseError::DecodingFailure {
                                    ctx: ErrorContext::default(),
                                    source: Error::new(
                                        ErrorKind::InvalidData,
                                        "locations in result is not expected type",
                                    ),
                                })
                            }
                        },
                        None => {
                            return Err(CouchbaseError::DecodingFailure {
                                ctx: ErrorContext::default(),
                                source: Error::new(ErrorKind::InvalidData, "missing field `score`"),
                            })
                        }
                    };
                    let fields = decoded.get("fields").cloned();
                    let locations = match decoded.get("locations") {
                        Some(i) => Some(SearchRowLocations::try_from(i)?),
                        None => None,
                    };
                    let fragments = decoded.get("fragments").cloned();

                    Ok(SearchRow {
                        index,
                        id,
                        score,
                        fields,
                        locations,
                        fragments,
                    })
                }
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> CouchbaseResult<SearchMetaData> {
        self.meta
            .take()
            .expect("Can not consume metadata twice!")
            .await
            .map_err(|e| {
                let mut ctx = ErrorContext::default();
                ctx.insert("error", Value::String(e.to_string()));
                CouchbaseError::RequestCanceled { ctx }
            })
    }

    pub async fn facets(&mut self) -> CouchbaseResult<HashMap<String, SearchFacetResult>> {
        match self.facets.take().unwrap().await {
            Ok(val) => {
                if val.is_null() {
                    return Ok(HashMap::new());
                }
                let val = val.as_object().unwrap();
                let mut res: HashMap<String, SearchFacetResult> = HashMap::new();
                for item in val.iter() {
                    if item.1.get("date_ranges").is_some() {
                        res.insert(
                            item.0.clone(),
                            SearchFacetResult::DateRangeSearchFacetResult(
                                DateRangeSearchFacetResult::try_from(item)?,
                            ),
                        );
                    } else if item.1.get("numeric_ranges").is_some() {
                        res.insert(
                            item.0.clone(),
                            SearchFacetResult::NumericRangeSearchFacetResult(
                                NumericRangeSearchFacetResult::try_from(item)?,
                            ),
                        );
                    } else {
                        res.insert(
                            item.0.clone(),
                            SearchFacetResult::TermSearchFacetResult(
                                TermSearchFacetResult::try_from(item)?,
                            ),
                        );
                    }
                }
                Ok(res)
            }
            Err(_e) => Err(CouchbaseError::RequestCanceled {
                ctx: ErrorContext::default(),
            }),
        }
    }
}
