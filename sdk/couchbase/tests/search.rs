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

use crate::common::consistency_utils::{verify_collection_created, verify_scope_created};
use crate::common::doc_generation::{import_color_sample, import_sample_beer_dataset};
use crate::common::features::TestFeatureCode;
use crate::common::test_config::run_test;
use crate::common::{new_key, try_until};
use chrono::DateTime;
use couchbase::management::collections::collection_settings::CreateCollectionSettings;
use couchbase::management::search::index::SearchIndex;
use couchbase::options::search_options::SearchOptions;
use couchbase::results::search_results::{SearchFacetResultType, SearchResult, SearchRow};
use couchbase::scope::Scope;
use couchbase::search::facets::{
    DateRange, DateRangeFacet, Facet, NumericRange, NumericRangeFacet, TermFacet,
};
use couchbase::search::queries::{MatchQuery, Query, TermQuery};
use couchbase::search::request::SearchRequest;
use couchbase::search::sort::{Sort, SortId};
use couchbase::search::vector::{VectorQuery, VectorSearch};
use futures::StreamExt;
use log::{error, warn};
use std::collections::HashMap;
use std::ops::Add;
use std::time::Duration;
use tokio::time;
use tokio::time::{timeout_at, Instant};

mod common;

#[test]
fn test_search_basic() {
    run_test(async |cluster, bucket| {
        if !cluster.supports_feature(&TestFeatureCode::SearchManagementCollections) {
            return;
        }

        let scope_name = new_key();
        let collection_name = new_key();

        let bucket = bucket;
        let collection_mgr = bucket.collections();
        collection_mgr
            .create_scope(&scope_name, None)
            .await
            .unwrap();
        verify_scope_created(&collection_mgr, &scope_name).await;

        collection_mgr
            .create_collection(
                &scope_name,
                &collection_name,
                CreateCollectionSettings::new(),
                None,
            )
            .await
            .unwrap();
        verify_collection_created(&collection_mgr, &scope_name, &collection_name).await;

        let scope = bucket.scope(&scope_name);
        let collection = scope.collection(&collection_name);

        let index_name = index_name();

        import_search_index(
            "tests/testdata/basic_scoped_search_index.json",
            &scope,
            &index_name,
            cluster.default_bucket(),
            &scope_name,
            &collection_name,
        )
        .await;
        let import_results = import_sample_beer_dataset("search", &collection).await;

        let query = TermQuery::new("search").field("service".to_string());

        let mut facets = HashMap::new();
        facets.insert(
            "type".to_string(),
            Facet::Term(TermFacet::new("country").size(5)),
        );
        facets.insert(
            "date".to_string(),
            Facet::DateRange(
                DateRangeFacet::new("updated").size(5).add_date_range(
                    DateRange::new("updated")
                        .start(DateTime::parse_from_rfc3339("2000-07-22 20:00:20Z").unwrap())
                        .end(DateTime::parse_from_rfc3339("2020-07-22 20:00:20Z").unwrap()),
                ),
            ),
        );
        facets.insert(
            "numeric".to_string(),
            Facet::NumericRange(
                NumericRangeFacet::new("geo.lat")
                    .size(5)
                    .add_numeric_range(NumericRange::new("lat").min(30f64).max(31f64)),
            ),
        );

        let sort = Sort::Id(SortId::new().descending(true));

        let deadline = Instant::now() + Duration::from_secs(60);

        let res: SearchResult;
        let rows: Vec<SearchRow>;
        loop {
            let mut this_res = match scope
                .search(
                    &index_name,
                    SearchRequest::with_search_query(Query::Term(query.clone())),
                    SearchOptions::new()
                        .include_locations(true)
                        .server_timeout(Duration::from_secs(10))
                        .facets(facets.clone())
                        .sort(vec![sort.clone()])
                        .fields(vec!["city".to_string()]),
                )
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    error!("search failed: {e}");
                    let sleep = time::sleep(Duration::from_secs(1));
                    timeout_at(deadline, sleep).await.unwrap();

                    continue;
                }
            };

            let mut this_rows = vec![];
            while let Some(row) = this_res.rows().next().await {
                this_rows.push(row.unwrap());
            }

            if this_rows.len() == import_results.len() {
                rows = this_rows.clone();
                res = this_res;
                break;
            }

            error!(
                "search returned {} rows, expected {}",
                this_rows.len(),
                import_results.len()
            );

            let sleep = time::sleep(Duration::from_secs(1));
            timeout_at(deadline, sleep).await.unwrap();
        }

        scope
            .search_indexes()
            .drop_index(&index_name, None)
            .await
            .unwrap();

        for row in rows {
            let locations = row.locations.as_ref().unwrap();
            let location = locations.get_by_field_and_term("service", "search");
            for loc in location {
                assert_eq!(0, loc.start);
                assert_ne!(0, loc.end);
                assert!(loc.array_positions.is_none());
            }

            assert!(!row.id.is_empty());
            assert!(!row.index.is_empty());

            let fields: HashMap<String, String> = row.fields().unwrap();
            assert_eq!(1, fields.len());
            assert!(!fields.get("city").unwrap().is_empty());
        }

        let metadata = res.metadata().unwrap();
        let facets = res.facets().unwrap();

        assert!(metadata.errors.is_empty());
        assert!(!metadata.metrics.took.is_zero());
        assert_ne!(0, metadata.metrics.total_hits);
        assert!(metadata.metrics.max_score > 0.0);
        assert_ne!(0, metadata.metrics.successful_partition_count);
        assert_ne!(0, metadata.metrics.total_partition_count);
        assert_eq!(0, metadata.metrics.failed_partition_count);

        let types = facets.get(&String::from("type")).unwrap();
        assert_eq!(7, types.total);
        assert_eq!(0, types.missing);
        assert_eq!(0, types.other);
        assert_eq!("country", types.field);
        assert_eq!("type", types.name);
        match &types.facets {
            SearchFacetResultType::TermFacets(terms) => {
                assert_eq!(4, terms.len());
                for term in terms {
                    match term.term {
                        "belgium" => {
                            assert_eq!(2, term.count);
                        }
                        "states" => {
                            assert_eq!(2, term.count);
                        }
                        "united" => {
                            assert_eq!(2, term.count);
                        }
                        "norway" => {
                            assert_eq!(1, term.count);
                        }
                        _ => panic!("unexpected term"),
                    }
                }
            }
            SearchFacetResultType::NumericRangeFacets(_) => {
                panic!("expected term facet")
            }
            SearchFacetResultType::DateRangeFacets(_) => {
                panic!("expected term facet")
            }
            _ => panic!("unexpected facet type"),
        }

        let dates = facets.get(&String::from("date")).unwrap();
        assert_eq!(5, dates.total);
        assert_eq!(0, dates.missing);
        assert_eq!(0, dates.other);
        assert_eq!("updated", dates.field);
        assert_eq!("date", dates.name);
        match &dates.facets {
            SearchFacetResultType::TermFacets(_) => {
                panic!("expected date range facet")
            }
            SearchFacetResultType::NumericRangeFacets(_) => {
                panic!("expected date range facet")
            }
            SearchFacetResultType::DateRangeFacets(date_ranges) => {
                assert_eq!(1, date_ranges.len());
                let range = &date_ranges[0];
                assert_eq!(5, range.count);
                assert_eq!("updated", range.name);
                assert_eq!("2000-07-22T20:00:20+00:00", range.start.to_rfc3339());
                assert_eq!("2020-07-22T20:00:20+00:00", range.end.to_rfc3339());
            }
            _ => panic!("unexpected facet type"),
        }

        let numeric = facets.get(&String::from("numeric")).unwrap();
        assert_eq!(1, numeric.total);
        assert_eq!(0, numeric.missing);
        assert_eq!(0, numeric.other);
        assert_eq!("geo.lat", numeric.field);
        assert_eq!("numeric", numeric.name);
        match &numeric.facets {
            SearchFacetResultType::TermFacets(_) => {
                panic!("expected numeric range facet")
            }
            SearchFacetResultType::NumericRangeFacets(numeric_ranges) => {
                assert_eq!(1, numeric_ranges.len());
                let range = &numeric_ranges[0];
                assert_eq!(1, range.count);
                assert_eq!("lat", range.name);
                assert_eq!(30f64, range.min);
                assert_eq!(31f64, range.max);
            }
            SearchFacetResultType::DateRangeFacets(_) => {
                panic!("expected numeric range facet")
            }
            _ => panic!("unexpected facet type"),
        }
    })
}

#[test]
fn test_search_vector() {
    run_test(async |cluster, bucket| {
        if !cluster.supports_feature(&TestFeatureCode::SearchManagementCollections)
            || !cluster.supports_feature(&TestFeatureCode::VectorSearch)
        {
            return;
        }

        let scope_name = new_key();
        let collection_name = new_key();

        let bucket = bucket;
        let collection_mgr = bucket.collections();
        collection_mgr
            .create_scope(&scope_name, None)
            .await
            .unwrap();
        verify_scope_created(&collection_mgr, &scope_name).await;

        collection_mgr
            .create_collection(
                &scope_name,
                &collection_name,
                CreateCollectionSettings::new(),
                None,
            )
            .await
            .unwrap();
        verify_collection_created(&collection_mgr, &scope_name, &collection_name).await;

        let scope = bucket.scope(&scope_name);
        let collection = scope.collection(&collection_name);

        let index_name = index_name();

        import_search_index(
            "tests/testdata/scoped_vector_index.json",
            &scope,
            &index_name,
            cluster.default_bucket(),
            &scope_name,
            &collection_name,
        )
        .await;

        let import_results = import_color_sample("search", &collection).await;

        let query = MatchQuery::new("primary").field("color_wheel_pos".to_string());
        let expected_rows = 2;
        let sort = Sort::Id(SortId::new().descending(true));

        let deadline = Instant::now() + Duration::from_secs(60);

        let res: SearchResult;
        let rows: Vec<SearchRow>;
        loop {
            let vector_query = VectorQuery::with_vector("color_rgb", vec![255.0, 255.0, 255.0])
                .prefilter(Query::Match(query.clone()))
                .num_candidates(10);

            let mut this_res = match scope
                .search(
                    &index_name,
                    SearchRequest::with_vector_search(VectorSearch::new(vec![vector_query], None)),
                    SearchOptions::new()
                        .include_locations(true)
                        .server_timeout(Duration::from_secs(10))
                        .sort(vec![sort.clone()]),
                )
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    error!("search failed: {e}");
                    let sleep = time::sleep(Duration::from_secs(1));
                    timeout_at(deadline, sleep).await.unwrap();

                    continue;
                }
            };

            let mut this_rows = vec![];
            while let Some(row) = this_res.rows().next().await {
                this_rows.push(row.unwrap());
            }

            if this_rows.len() == expected_rows {
                rows = this_rows.clone();
                res = this_res;
                break;
            }

            error!(
                "search returned {} rows, expected {}",
                this_rows.len(),
                expected_rows
            );

            let sleep = time::sleep(Duration::from_secs(1));
            timeout_at(deadline, sleep).await.unwrap();
        }

        scope
            .search_indexes()
            .drop_index(&index_name, None)
            .await
            .unwrap();

        for row in rows {
            assert!(!row.id.is_empty());
            assert!(!row.index.is_empty());
        }

        // Metadata assertions
        let metadata = res.metadata().unwrap();

        assert!(metadata.errors.is_empty());
        assert!(!metadata.metrics.took.is_zero());
        assert_eq!(expected_rows as u64, metadata.metrics.total_hits);
        assert!(metadata.metrics.max_score >= 0.0);
        assert_eq!(1, metadata.metrics.successful_partition_count);
        assert_eq!(1, metadata.metrics.total_partition_count);
        assert_eq!(0, metadata.metrics.failed_partition_count);
    })
}

async fn import_search_index(
    index_definition_path: &'static str,
    scope: &Scope,
    index_name: &str,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) {
    let mut data = std::fs::read_to_string(index_definition_path).unwrap();
    let mut data = data.replace("$indexName", index_name);
    let mut data = data.replace("$bucketName", bucket_name);
    let mut data = data.replace("$scopeName", scope_name);
    let data = data.replace("$collectionName", collection_name);

    let mut index: SearchIndex = serde_json::from_str(&data).unwrap();

    let mgr = scope.search_indexes();

    try_until(
        Instant::now().add(Duration::from_secs(30)),
        Duration::from_millis(500),
        "failed to upsert index",
        || async {
            match mgr.upsert_index(index.clone(), None).await {
                Ok(_) => Ok(Some(())),
                Err(e) => {
                    warn!("failed to upsert index: {e}");
                    Ok(None)
                }
            }
        },
    )
    .await;
}

fn index_name() -> String {
    let mut name = new_key();
    loop {
        if name.as_bytes()[0].is_ascii_digit() {
            name = name[1..].to_string();
        } else {
            break;
        }
    }

    name
}
