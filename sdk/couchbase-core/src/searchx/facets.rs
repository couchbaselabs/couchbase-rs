use chrono::{DateTime, FixedOffset};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TermFacet {
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct NumericRange {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct NumericRangeFacet {
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
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

impl Serialize for DateRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("name", &self.name)?;
        if let Some(start) = &self.start {
            map.serialize_entry("start", &start.to_rfc3339())?;
        }
        if let Some(end) = &self.end {
            map.serialize_entry("end", &end.to_rfc3339())?;
        }

        map.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct DateRangeFacet {
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Facet {
    Term(TermFacet),
    NumericRange(NumericRangeFacet),
    DateRange(DateRangeFacet),
}
