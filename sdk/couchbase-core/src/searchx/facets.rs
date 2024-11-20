use chrono::{DateTime, FixedOffset};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermFacet {
    #[builder(!default)]
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct NumericRange {
    #[builder(!default)]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct NumericRangeFacet {
    #[builder(!default)]
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[builder(mutators(
        pub fn add_numeric_range(&mut self, range: NumericRange) {
            self.numeric_ranges.push(range);
        }
    ))]
    pub numeric_ranges: Vec<NumericRange>,
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DateRange {
    #[builder(!default)]
    pub name: String,
    pub start: Option<DateTime<FixedOffset>>,
    pub end: Option<DateTime<FixedOffset>>,
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

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DateRangeFacet {
    #[builder(!default)]
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[builder(mutators(
        pub fn add_date_range(&mut self, range: DateRange) {
            self.date_ranges.push(range);
        }
    ))]
    pub date_ranges: Vec<DateRange>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Facet {
    Term(TermFacet),
    NumericRange(NumericRangeFacet),
    DateRange(DateRangeFacet),
}
