use crate::search::location::Location;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortScore {
    pub descending: Option<bool>,
}

impl From<SortScore> for couchbase_core::searchx::sort::SortScore {
    fn from(sort_score: SortScore) -> Self {
        couchbase_core::searchx::sort::SortScore::builder()
            .descending(sort_score.descending)
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SortId {
    pub descending: Option<bool>,
}

impl From<SortId> for couchbase_core::searchx::sort::SortId {
    fn from(sort_id: SortId) -> Self {
        couchbase_core::searchx::sort::SortId::builder()
            .descending(sort_id.descending)
            .build()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldType {
    Auto,
    String,
    Number,
    Date,
}

impl From<SortFieldType> for couchbase_core::searchx::sort::SortFieldType {
    fn from(sort_field_type: SortFieldType) -> Self {
        match sort_field_type {
            SortFieldType::Auto => couchbase_core::searchx::sort::SortFieldType::Auto,
            SortFieldType::String => couchbase_core::searchx::sort::SortFieldType::String,
            SortFieldType::Number => couchbase_core::searchx::sort::SortFieldType::Number,
            SortFieldType::Date => couchbase_core::searchx::sort::SortFieldType::Date,
        }
    }
}

impl From<SortFieldType> for Option<couchbase_core::searchx::sort::SortFieldType> {
    fn from(sort_field_type: SortFieldType) -> Self {
        Some(sort_field_type.into())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldMode {
    Default,
    Min,
    Max,
}

impl From<SortFieldMode> for couchbase_core::searchx::sort::SortFieldMode {
    fn from(sort_field_mode: SortFieldMode) -> Self {
        match sort_field_mode {
            SortFieldMode::Default => couchbase_core::searchx::sort::SortFieldMode::Default,
            SortFieldMode::Min => couchbase_core::searchx::sort::SortFieldMode::Min,
            SortFieldMode::Max => couchbase_core::searchx::sort::SortFieldMode::Max,
        }
    }
}

impl From<SortFieldMode> for Option<couchbase_core::searchx::sort::SortFieldMode> {
    fn from(sort_field_mode: SortFieldMode) -> Self {
        Some(sort_field_mode.into())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldMissing {
    First,
    Last,
}

impl From<SortFieldMissing> for couchbase_core::searchx::sort::SortFieldMissing {
    fn from(sort_field_missing: SortFieldMissing) -> Self {
        match sort_field_missing {
            SortFieldMissing::First => couchbase_core::searchx::sort::SortFieldMissing::First,
            SortFieldMissing::Last => couchbase_core::searchx::sort::SortFieldMissing::Last,
        }
    }
}

impl From<SortFieldMissing> for Option<couchbase_core::searchx::sort::SortFieldMissing> {
    fn from(sort_field_missing: SortFieldMissing) -> Self {
        Some(sort_field_missing.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
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

impl From<SortField> for couchbase_core::searchx::sort::SortField {
    fn from(sort_field: SortField) -> Self {
        couchbase_core::searchx::sort::SortField::builder()
            .field(sort_field.field)
            .descending(sort_field.descending)
            .sort_type(sort_field.sort_type.map(|st| st.into()))
            .mode(sort_field.mode.map(|m| m.into()))
            .missing(sort_field.missing.map(|m| m.into()))
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl From<SortGeoDistanceUnit> for couchbase_core::searchx::sort::SortGeoDistanceUnit {
    fn from(sort_geo_distance_unit: SortGeoDistanceUnit) -> Self {
        match sort_geo_distance_unit {
            SortGeoDistanceUnit::Meters => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::Meters
            }
            SortGeoDistanceUnit::Miles => couchbase_core::searchx::sort::SortGeoDistanceUnit::Miles,
            SortGeoDistanceUnit::Centimeters => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::Centimeters
            }
            SortGeoDistanceUnit::Millimeters => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::Millimeters
            }
            SortGeoDistanceUnit::NauticalMiles => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::NauticalMiles
            }
            SortGeoDistanceUnit::Kilometers => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::Kilometers
            }
            SortGeoDistanceUnit::Feet => couchbase_core::searchx::sort::SortGeoDistanceUnit::Feet,
            SortGeoDistanceUnit::Yards => couchbase_core::searchx::sort::SortGeoDistanceUnit::Yards,
            SortGeoDistanceUnit::Inches => {
                couchbase_core::searchx::sort::SortGeoDistanceUnit::Inches
            }
        }
    }
}

impl From<SortGeoDistanceUnit> for Option<couchbase_core::searchx::sort::SortGeoDistanceUnit> {
    fn from(sort_geo_distance_unit: SortGeoDistanceUnit) -> Self {
        Some(sort_geo_distance_unit.into())
    }
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct SortGeoDistance {
    pub field: String,
    #[builder(default)]
    pub descending: Option<bool>,
    pub location: Location,
    #[builder(default)]
    pub unit: Option<SortGeoDistanceUnit>,
}

impl From<SortGeoDistance> for couchbase_core::searchx::sort::SortGeoDistance {
    fn from(sort_geo_distance: SortGeoDistance) -> Self {
        couchbase_core::searchx::sort::SortGeoDistance::builder()
            .field(sort_geo_distance.field)
            .descending(sort_geo_distance.descending)
            .location(sort_geo_distance.location)
            .unit(sort_geo_distance.unit.map(|u| u.into()))
            .build()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Sort {
    Score(SortScore),
    Id(SortId),
    Field(SortField),
    GeoDistance(SortGeoDistance),
}

impl From<Sort> for couchbase_core::searchx::sort::Sort {
    fn from(sort: Sort) -> Self {
        match sort {
            Sort::Score(s) => couchbase_core::searchx::sort::Sort::Score(s.into()),
            Sort::Id(i) => couchbase_core::searchx::sort::Sort::Id(i.into()),
            Sort::Field(f) => couchbase_core::searchx::sort::Sort::Field(f.into()),
            Sort::GeoDistance(g) => couchbase_core::searchx::sort::Sort::GeoDistance(g.into()),
        }
    }
}

impl From<Sort> for Option<couchbase_core::searchx::sort::Sort> {
    fn from(sort: Sort) -> Self {
        Some(sort.into())
    }
}
