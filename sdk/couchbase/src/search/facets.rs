/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

//! Facet definitions for aggregating Full-Text Search results.
//!
//! Facets allow you to aggregate search results into categories. There are three types:
//!
//! - [`TermFacet`] — groups results by distinct terms in a field.
//! - [`NumericRangeFacet`] — groups results by numeric ranges.
//! - [`DateRangeFacet`] — groups results by date ranges.
//!
//! Use the [`Facet`] enum to pass facets to
//! [`SearchOptions::facets`](crate::options::search_options::SearchOptions::facets).

use chrono::{DateTime, FixedOffset};

/// A term facet that groups search results by distinct terms in a field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TermFacet {
    /// The field to aggregate.
    pub field: String,
    /// Maximum number of term buckets to return.
    pub size: Option<u64>,
}

impl TermFacet {
    /// Creates a new `TermFacet` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
        }
    }

    /// Sets the maximum number of term buckets to return.
    pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.size = size.into();
        self
    }
}

impl From<TermFacet> for couchbase_core::searchx::facets::TermFacet {
    fn from(facet: TermFacet) -> Self {
        couchbase_core::searchx::facets::TermFacet::new(facet.field).size(facet.size)
    }
}

/// A named numeric range used in a [`NumericRangeFacet`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRange {
    /// The name of this range bucket.
    pub name: String,
    /// The minimum value (inclusive) of the range.
    pub min: Option<f64>,
    /// The maximum value (exclusive) of the range.
    pub max: Option<f64>,
}

impl NumericRange {
    /// Creates a new `NumericRange` with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            min: None,
            max: None,
        }
    }

    /// Sets the minimum value of the range.
    pub fn min(mut self, min: impl Into<Option<f64>>) -> Self {
        self.min = min.into();
        self
    }

    /// Sets the maximum value of the range.
    pub fn max(mut self, max: impl Into<Option<f64>>) -> Self {
        self.max = max.into();
        self
    }
}

impl From<NumericRange> for couchbase_core::searchx::facets::NumericRange {
    fn from(facet: NumericRange) -> Self {
        couchbase_core::searchx::facets::NumericRange::new(facet.name)
            .min(facet.min)
            .max(facet.max)
    }
}

/// A numeric range facet that groups search results by numeric value ranges.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRangeFacet {
    /// The field to aggregate.
    pub field: String,
    /// Maximum number of range buckets to return.
    pub size: Option<u64>,
    /// The numeric ranges to group by.
    pub numeric_ranges: Vec<NumericRange>,
}

impl NumericRangeFacet {
    /// Creates a new empty `NumericRangeFacet` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
            numeric_ranges: Vec::new(),
        }
    }

    /// Creates a `NumericRangeFacet` with pre-defined ranges.
    pub fn with_numeric_ranges(
        field: impl Into<String>,
        numeric_ranges: impl Into<Vec<NumericRange>>,
    ) -> Self {
        Self {
            field: field.into(),
            size: None,
            numeric_ranges: numeric_ranges.into(),
        }
    }

    /// Sets the maximum number of range buckets to return.
    pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.size = size.into();
        self
    }

    /// Adds a numeric range to this facet.
    pub fn add_numeric_range(mut self, range: NumericRange) -> Self {
        self.numeric_ranges.push(range);
        self
    }
}

impl From<NumericRangeFacet> for couchbase_core::searchx::facets::NumericRangeFacet {
    fn from(facet: NumericRangeFacet) -> Self {
        couchbase_core::searchx::facets::NumericRangeFacet::with_numeric_ranges(
            facet.field,
            facet
                .numeric_ranges
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::facets::NumericRange>>(),
        )
        .size(facet.size)
    }
}

/// A named date range used in a [`DateRangeFacet`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRange {
    /// The name of this range bucket.
    pub name: String,
    /// The start date/time (inclusive) of the range.
    pub start: Option<DateTime<FixedOffset>>,
    /// The end date/time (exclusive) of the range.
    pub end: Option<DateTime<FixedOffset>>,
}

impl DateRange {
    /// Creates a new `DateRange` with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: None,
            end: None,
        }
    }

    /// Sets the start date/time of the range.
    pub fn start(mut self, start: impl Into<Option<DateTime<FixedOffset>>>) -> Self {
        self.start = start.into();
        self
    }

    /// Sets the end date/time of the range.
    pub fn end(mut self, end: impl Into<Option<DateTime<FixedOffset>>>) -> Self {
        self.end = end.into();
        self
    }
}

impl From<DateRange> for couchbase_core::searchx::facets::DateRange {
    fn from(facet: DateRange) -> Self {
        couchbase_core::searchx::facets::DateRange::new(facet.name)
            .start(facet.start)
            .end(facet.end)
    }
}

/// A date range facet that groups search results by date/time ranges.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRangeFacet {
    /// The field to aggregate.
    pub field: String,
    /// Maximum number of range buckets to return.
    pub size: Option<u64>,
    /// The date ranges to group by.
    pub date_ranges: Vec<DateRange>,
}

impl DateRangeFacet {
    /// Creates a new empty `DateRangeFacet` for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
            date_ranges: Vec::new(),
        }
    }

    /// Creates a `DateRangeFacet` with pre-defined date ranges.
    pub fn with_date_ranges(
        field: impl Into<String>,
        date_ranges: impl Into<Vec<DateRange>>,
    ) -> Self {
        Self {
            field: field.into(),
            size: None,
            date_ranges: date_ranges.into(),
        }
    }

    /// Sets the maximum number of range buckets to return.
    pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.size = size.into();
        self
    }

    /// Adds a date range to this facet.
    pub fn add_date_range(mut self, range: DateRange) -> Self {
        self.date_ranges.push(range);
        self
    }
}

impl From<DateRangeFacet> for couchbase_core::searchx::facets::DateRangeFacet {
    fn from(facet: DateRangeFacet) -> Self {
        couchbase_core::searchx::facets::DateRangeFacet::with_date_ranges(
            facet.field,
            facet
                .date_ranges
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::facets::DateRange>>(),
        )
        .size(facet.size)
    }
}

/// A search facet for aggregating search results.
///
/// Use with [`SearchOptions::facets`](crate::options::search_options::SearchOptions::facets).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Facet {
    /// A term facet.
    Term(TermFacet),
    /// A numeric range facet.
    NumericRange(NumericRangeFacet),
    /// A date range facet.
    DateRange(DateRangeFacet),
}

impl From<Facet> for couchbase_core::searchx::facets::Facet {
    fn from(facet: Facet) -> Self {
        match facet {
            Facet::Term(f) => couchbase_core::searchx::facets::Facet::Term(f.into()),
            Facet::NumericRange(f) => {
                couchbase_core::searchx::facets::Facet::NumericRange(f.into())
            }
            Facet::DateRange(f) => couchbase_core::searchx::facets::Facet::DateRange(f.into()),
        }
    }
}
