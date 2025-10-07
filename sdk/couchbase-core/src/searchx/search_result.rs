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

use chrono::{DateTime, FixedOffset};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct HitLocation {
    pub start: u32,
    pub end: u32,
    pub position: u32,
    pub array_positions: Option<Vec<u32>>,
}

#[derive(Debug, Clone)]
pub struct ResultHit {
    pub index: String,
    pub id: String,
    pub score: f64,
    pub locations: Option<HashMap<String, HashMap<String, Vec<HitLocation>>>>,
    pub fragments: Option<HashMap<String, Vec<String>>>,
    pub fields: Option<Box<RawValue>>,
    pub explanation: Option<Box<RawValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetaData {
    pub errors: HashMap<String, String>,
    pub metrics: Metrics,
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct Metrics {
    pub failed_partition_count: u64,
    pub max_score: f64,
    pub successful_partition_count: u64,
    pub took: Duration,
    pub total_hits: u64,
    pub total_partition_count: u64,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TermFacetResult {
    pub term: String,
    pub count: i64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct NumericRangeFacetResult {
    pub name: String,
    pub min: f64,
    pub max: f64,
    pub count: i64,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DateRangeFacetResult {
    pub name: String,
    pub start: DateTime<FixedOffset>,
    pub end: DateTime<FixedOffset>,
    pub count: i64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct FacetResult {
    pub field: String,
    pub total: i64,
    pub missing: i64,
    pub other: i64,
    pub terms: Option<Vec<TermFacetResult>>,
    pub numeric_ranges: Option<Vec<NumericRangeFacetResult>>,
    pub date_ranges: Option<Vec<DateRangeFacetResult>>,
}
