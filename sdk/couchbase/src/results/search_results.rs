use crate::error;
use chrono::{DateTime, FixedOffset};
use couchbase_core::searchcomponent::SearchResultStream;
use couchbase_core::searchx;
use couchbase_core::searchx::search_result::ResultHit;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct SearchResult {
    wrapped: SearchResultStream,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchMetrics {
    pub took: Duration,
    pub total_hits: u64,
    pub max_score: f64,
    pub successful_partition_count: u64,
    pub failed_partition_count: u64,
    pub total_partition_count: u64,
}

impl From<&searchx::search_result::Metrics> for SearchMetrics {
    fn from(metrics: &searchx::search_result::Metrics) -> Self {
        Self {
            took: metrics.took,
            total_hits: metrics.total_hits,
            max_score: metrics.max_score,
            successful_partition_count: metrics.successful_partition_count,
            failed_partition_count: metrics.failed_partition_count,
            total_partition_count: metrics.total_partition_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchMetaData<'a> {
    pub errors: &'a HashMap<String, String>,
    pub metrics: SearchMetrics,
}

impl<'a> From<&'a searchx::search_result::MetaData> for SearchMetaData<'a> {
    fn from(meta: &'a searchx::search_result::MetaData) -> Self {
        Self {
            errors: &meta.errors,
            metrics: SearchMetrics::from(&meta.metrics),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TermFacetResult<'a> {
    pub term: &'a str,
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::TermFacetResult> for TermFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::TermFacetResult) -> Self {
        Self {
            term: &facet.term,
            count: facet.count,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct NumericRangeFacetResult<'a> {
    pub name: &'a str,
    pub min: f64,
    pub max: f64,
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::NumericRangeFacetResult> for NumericRangeFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::NumericRangeFacetResult) -> Self {
        Self {
            name: &facet.name,
            min: facet.min,
            max: facet.max,
            count: facet.count,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DateRangeFacetResult<'a> {
    pub name: &'a str,
    pub start: &'a DateTime<FixedOffset>,
    pub end: &'a DateTime<FixedOffset>,
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::DateRangeFacetResult> for DateRangeFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::DateRangeFacetResult) -> Self {
        Self {
            name: &facet.name,
            start: &facet.start,
            end: &facet.end,
            count: facet.count,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum SearchFacetResultType<'a> {
    TermFacets(Vec<TermFacetResult<'a>>),
    NumericRangeFacets(Vec<NumericRangeFacetResult<'a>>),
    DateRangeFacets(Vec<DateRangeFacetResult<'a>>),
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct SearchFacetResult<'a> {
    pub name: &'a str,
    pub field: &'a str,
    pub total: u64,
    pub missing: u64,
    pub other: u64,
    pub facets: SearchFacetResultType<'a>,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SearchRowLocation {
    pub field: String,
    pub term: String,
    pub start: u32,
    pub end: u32,
    pub position: u32,
    pub array_positions: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchRowLocations {
    locations: Vec<SearchRowLocation>,
}

impl SearchRowLocations {
    pub fn get_all(&self) -> &[SearchRowLocation] {
        &self.locations
    }

    pub fn get_by_field(&self, field: &str) -> Vec<&SearchRowLocation> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field)
            .collect()
    }

    pub fn get_by_field_and_term(&self, field: &str, term: &str) -> Vec<&SearchRowLocation> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field && loc.term == term)
            .collect()
    }

    pub fn fields(&self) -> Vec<&str> {
        self.locations
            .iter()
            .map(|loc| loc.field.as_str())
            .collect()
    }

    pub fn terms(&self) -> Vec<&str> {
        self.locations.iter().map(|loc| loc.term.as_str()).collect()
    }

    pub fn terms_for(&self, field: &str) -> Vec<&str> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field)
            .map(|loc| loc.term.as_str())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub index: String,
    pub id: String,
    pub score: f64,
    pub explanation: Option<Box<RawValue>>,
    pub locations: Option<SearchRowLocations>,
    pub fragments: Option<HashMap<String, Vec<String>>>,
    fields: Option<Box<RawValue>>,
}

impl SearchRow {
    pub fn fields<V: DeserializeOwned>(&self) -> error::Result<V> {
        if let Some(fields) = &self.fields {
            serde_json::from_str(fields.get()).map_err(|e| error::Error { msg: e.to_string() })
        } else {
            Err(error::Error {
                msg: "no fields in response".to_string(),
            })
        }
    }
}

impl From<ResultHit> for SearchRow {
    fn from(hit: ResultHit) -> Self {
        let locations = if let Some(hit_locations) = hit.locations {
            let mut locations = vec![];
            for (field_name, field_data) in hit_locations {
                for (term_name, term_data) in field_data {
                    for (loc_idx, loc_data) in term_data.into_iter().enumerate() {
                        locations.push(SearchRowLocation {
                            field: field_name.clone(),
                            term: term_name.clone(),
                            start: loc_data.start,
                            end: loc_data.end,
                            position: loc_idx as u32,
                            array_positions: loc_data.array_positions,
                        });
                    }
                }
            }

            Some(SearchRowLocations { locations })
        } else {
            None
        };

        Self {
            index: hit.index,
            id: hit.id,
            score: hit.score,
            explanation: hit.explanation,
            locations,
            fragments: hit.fragments,
            fields: hit.fields,
        }
    }
}

impl From<SearchResultStream> for SearchResult {
    fn from(wrapped: SearchResultStream) -> Self {
        Self { wrapped }
    }
}

struct SearchRows<'a> {
    wrapped: &'a mut SearchResultStream,
}

impl<'a> Stream for SearchRows<'a> {
    type Item = error::Result<SearchRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let row = match self.wrapped.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row))) => row,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(Some(Ok(row.into())))
    }
}

impl SearchResult {
    pub fn metadata(&self) -> error::Result<SearchMetaData> {
        Ok(self.wrapped.metadata()?.into())
    }

    pub fn rows(&mut self) -> impl Stream<Item = error::Result<SearchRow>> + '_ {
        SearchRows {
            wrapped: &mut self.wrapped,
        }
    }

    pub fn facets(&self) -> error::Result<HashMap<&String, SearchFacetResult>> {
        let mut facets = HashMap::new();
        for (name, facet) in self.wrapped.facets()? {
            if let Some(facet_terms) = &facet.terms {
                let mut terms = vec![];
                for term in facet_terms {
                    terms.push(TermFacetResult::from(term));
                }

                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::TermFacets(terms),
                    },
                );
            } else if let Some(facet_dates) = &facet.date_ranges {
                let mut dates = vec![];
                for date in facet_dates {
                    dates.push(DateRangeFacetResult::from(date));
                }
                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::DateRangeFacets(dates),
                    },
                );
            } else if let Some(numeric) = &facet.numeric_ranges {
                let mut range = vec![];
                for num in numeric {
                    range.push(NumericRangeFacetResult::from(num));
                }
                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::NumericRangeFacets(range),
                    },
                );
            }
        }

        Ok(facets)
    }
}
