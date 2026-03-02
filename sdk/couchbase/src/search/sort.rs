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

//! Sort definitions for ordering Full-Text Search results.
//!
//! Search results can be sorted by score, document ID, field value, or geo-distance.
//! Use the [`Sort`] enum to pass sort specifications to
//! [`SearchOptions::sort`](crate::options::search_options::SearchOptions::sort).

use crate::search::location::Location;

/// Sort search results by relevance score.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SortScore {
    /// If `true`, sort in descending order (highest score first, which is the default).
    pub descending: Option<bool>,
}

impl SortScore {
    /// Creates a new `SortScore` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the sort direction.
    pub fn descending(mut self, descending: impl Into<Option<bool>>) -> Self {
        self.descending = descending.into();
        self
    }
}

impl From<SortScore> for couchbase_core::searchx::sort::SortScore {
    fn from(sort_score: SortScore) -> Self {
        couchbase_core::searchx::sort::SortScore::default().descending(sort_score.descending)
    }
}

/// Sort search results by document ID.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SortId {
    /// If `true`, sort in descending order.
    pub descending: Option<bool>,
}

impl SortId {
    /// Creates a new `SortId` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the sort direction.
    pub fn descending(mut self, descending: impl Into<Option<bool>>) -> Self {
        self.descending = descending.into();
        self
    }
}

impl From<SortId> for couchbase_core::searchx::sort::SortId {
    fn from(sort_id: SortId) -> Self {
        couchbase_core::searchx::sort::SortId::default().descending(sort_id.descending)
    }
}

/// The data type to use when sorting by a field value.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldType {
    /// Automatically detect the type (default).
    Auto,
    /// Sort as a string.
    String,
    /// Sort as a number.
    Number,
    /// Sort as a date.
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

/// How to handle multi-valued fields when sorting.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldMode {
    /// Use the server default behavior.
    Default,
    /// Use the minimum value.
    Min,
    /// Use the maximum value.
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

/// Where to place documents that are missing the sort field.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortFieldMissing {
    /// Place missing documents first.
    First,
    /// Place missing documents last.
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

/// Sort search results by a field value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SortField {
    /// The field to sort by.
    pub field: String,
    /// If `true`, sort in descending order.
    pub descending: Option<bool>,
    /// The data type to use for sorting.
    pub sort_type: Option<SortFieldType>,
    /// How to handle multi-valued fields.
    pub mode: Option<SortFieldMode>,
    /// Where to place documents missing the sort field.
    pub missing: Option<SortFieldMissing>,
}

impl SortField {
    /// Creates a new `SortField` for the given field name.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: None,
            sort_type: None,
            mode: None,
            missing: None,
        }
    }

    /// Sets the sort direction.
    pub fn descending(mut self, descending: impl Into<Option<bool>>) -> Self {
        self.descending = descending.into();
        self
    }

    /// Sets the data type for sorting.
    pub fn sort_type(mut self, sort_type: impl Into<Option<SortFieldType>>) -> Self {
        self.sort_type = sort_type.into();
        self
    }

    /// Sets how to handle multi-valued fields.
    pub fn mode(mut self, mode: impl Into<Option<SortFieldMode>>) -> Self {
        self.mode = mode.into();
        self
    }

    /// Sets where to place documents missing the sort field.
    pub fn missing(mut self, missing: impl Into<Option<SortFieldMissing>>) -> Self {
        self.missing = missing.into();
        self
    }
}

impl From<SortField> for couchbase_core::searchx::sort::SortField {
    fn from(sort_field: SortField) -> Self {
        couchbase_core::searchx::sort::SortField::new(sort_field.field)
            .descending(sort_field.descending)
            .sort_type(sort_field.sort_type.map(|st| st.into()))
            .mode(sort_field.mode.map(|m| m.into()))
            .missing(sort_field.missing.map(|m| m.into()))
    }
}

/// The unit of distance for geo-distance sorting.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortGeoDistanceUnit {
    /// Meters.
    Meters,
    /// Miles.
    Miles,
    /// Centimeters.
    Centimeters,
    /// Millimeters.
    Millimeters,
    /// Nautical miles.
    NauticalMiles,
    /// Kilometers.
    Kilometers,
    /// Feet.
    Feet,
    /// Yards.
    Yards,
    /// Inches.
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

/// Sort search results by geographic distance from a given location.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortGeoDistance {
    /// The geo field to compute distance from.
    pub field: String,
    /// If `true`, sort in descending order (farthest first).
    pub descending: Option<bool>,
    /// The origin location to compute distance from.
    pub location: Location,
    /// The unit for the distance calculation.
    pub unit: Option<SortGeoDistanceUnit>,
}

impl SortGeoDistance {
    /// Creates a new `SortGeoDistance` for the given field and origin location.
    pub fn new(field: impl Into<String>, location: Location) -> Self {
        Self {
            field: field.into(),
            location,
            descending: None,
            unit: None,
        }
    }

    /// Sets the sort direction.
    pub fn descending(mut self, descending: impl Into<Option<bool>>) -> Self {
        self.descending = descending.into();
        self
    }

    /// Sets the distance unit.
    pub fn unit(mut self, unit: impl Into<Option<SortGeoDistanceUnit>>) -> Self {
        self.unit = unit.into();
        self
    }
}

impl From<SortGeoDistance> for couchbase_core::searchx::sort::SortGeoDistance {
    fn from(sort_geo_distance: SortGeoDistance) -> Self {
        couchbase_core::searchx::sort::SortGeoDistance::new(
            sort_geo_distance.field,
            sort_geo_distance.location,
        )
        .descending(sort_geo_distance.descending)
        .unit(sort_geo_distance.unit.map(|u| u.into()))
    }
}

/// A search result sort specification.
///
/// Use with [`SearchOptions::sort`](crate::options::search_options::SearchOptions::sort).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Sort {
    /// Sort by relevance score.
    Score(SortScore),
    /// Sort by document ID.
    Id(SortId),
    /// Sort by a field value.
    Field(SortField),
    /// Sort by geographic distance.
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
