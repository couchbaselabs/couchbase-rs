use chrono::{DateTime, FixedOffset};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TermFacet {
    pub field: String,
    pub size: Option<u64>,
}

impl TermFacet {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
        }
    }

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

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRange {
    pub name: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl NumericRange {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            min: None,
            max: None,
        }
    }

    pub fn min(mut self, min: impl Into<Option<f64>>) -> Self {
        self.min = min.into();
        self
    }

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

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRangeFacet {
    pub field: String,
    pub size: Option<u64>,
    pub numeric_ranges: Vec<NumericRange>,
}

impl NumericRangeFacet {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
            numeric_ranges: Vec::new(),
        }
    }

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

    pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.size = size.into();
        self
    }

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

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRange {
    pub name: String,
    pub start: Option<DateTime<FixedOffset>>,
    pub end: Option<DateTime<FixedOffset>>,
}

impl DateRange {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: None,
            end: None,
        }
    }

    pub fn start(mut self, start: impl Into<Option<DateTime<FixedOffset>>>) -> Self {
        self.start = start.into();
        self
    }

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

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRangeFacet {
    pub field: String,
    pub size: Option<u64>,
    pub date_ranges: Vec<DateRange>,
}

impl DateRangeFacet {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            size: None,
            date_ranges: Vec::new(),
        }
    }

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

    pub fn size(mut self, size: impl Into<Option<u64>>) -> Self {
        self.size = size.into();
        self
    }

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

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Facet {
    Term(TermFacet),
    NumericRange(NumericRangeFacet),
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
