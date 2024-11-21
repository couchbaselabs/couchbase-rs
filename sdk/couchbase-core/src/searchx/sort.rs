use crate::searchx::query_options::Location;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortScore {
    pub descending: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortId {
    pub descending: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SortFieldType {
    Auto,
    String,
    Number,
    Date,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SortFieldMode {
    Default,
    Min,
    Max,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SortFieldMissing {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortField {
    #[builder(!default)]
    pub field: String,
    pub descending: Option<bool>,
    pub sort_type: Option<SortFieldType>,
    pub mode: Option<SortFieldMode>,
    pub missing: Option<SortFieldMissing>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum SortGeoDistanceUnit {
    Meters,
    Miles,
    Centimeters,
    Millimeters,
    NauticalMiles,
    Kilometers,
    Feet,
    Yards,
    Inches,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortGeoDistance {
    #[builder(!default)]
    pub field: String,
    pub descending: Option<bool>,
    #[builder(!default)]
    pub location: Location,
    pub unit: Option<SortGeoDistanceUnit>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Sort {
    Score(SortScore),
    Id(SortId),
    Field(SortField),
    GeoDistance(SortGeoDistance),
}

impl Serialize for Sort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        match self {
            Sort::Score(score) => {
                map.serialize_entry("by", "score")?;
                if let Some(desc) = score.descending {
                    map.serialize_entry("desc", &desc)?;
                }
            }
            Sort::Id(id) => {
                map.serialize_entry("by", "id")?;
                if let Some(desc) = id.descending {
                    map.serialize_entry("desc", &desc)?;
                }
            }
            Sort::Field(field) => {
                map.serialize_entry("by", "field")?;
                map.serialize_entry("field", &field.field)?;
                if let Some(desc) = field.descending {
                    map.serialize_entry("desc", &desc)?;
                }
                if let Some(sort_type) = &field.sort_type {
                    map.serialize_entry("type", &sort_type)?;
                }
                if let Some(mode) = &field.mode {
                    map.serialize_entry("mode", &mode)?;
                }
                if let Some(missing) = &field.missing {
                    map.serialize_entry("missing", &missing)?;
                }
            }
            Sort::GeoDistance(geo_distance) => {
                map.serialize_entry("by", "geo_distance")?;
                map.serialize_entry("field", &geo_distance.field)?;
                map.serialize_entry("location", &geo_distance.location)?;
                if let Some(desc) = geo_distance.descending {
                    map.serialize_entry("desc", &desc)?;
                }
                if let Some(unit) = &geo_distance.unit {
                    map.serialize_entry("unit", &unit)?;
                }
            }
        }

        map.end()
    }
}