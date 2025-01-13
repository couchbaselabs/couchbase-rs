use crate::common::create_cluster_from_test_config;
use crate::common::doc_generation::import_sample_beer_dataset;
use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use chrono::DateTime;
use couchbase::options::search_options::SearchOptions;
use couchbase::results::search_results::{SearchFacetResultType, SearchResult, SearchRow};
use couchbase::search::facets::{
    DateRange, DateRangeFacet, Facet, NumericRange, NumericRangeFacet, TermFacet,
};
use couchbase::search::queries::{Query, TermQuery};
use couchbase::search::request::SearchRequest;
use couchbase::search::sort::{Sort, SortId};
use futures::StreamExt;
use log::LevelFilter;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time;
use tokio::time::{timeout_at, Instant};

mod common;

const BASIC_INDEX_NAME: &str = "basic_search_index";

#[tokio::test]
async fn test_search_basic() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let scope = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await);

    let collection = scope.collection(test_collection().await);

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

    let deadline = Instant::now() + std::time::Duration::from_secs(60);

    let res: SearchResult;
    let rows: Vec<SearchRow>;
    loop {
        let mut this_res = cluster
            .search(
                BASIC_INDEX_NAME,
                SearchRequest::with_search_query(Query::Term(query.clone())),
                SearchOptions::new()
                    .include_locations(true)
                    .server_timeout(Duration::from_secs(5))
                    .facets(facets.clone())
                    .sort(vec![sort.clone()])
                    .fields(vec!["city".to_string()]),
            )
            .await
            .unwrap();

        let mut this_rows = vec![];
        while let Some(row) = this_res.rows().next().await {
            this_rows.push(row.unwrap());
        }

        if this_rows.len() == import_results.len() {
            rows = this_rows.clone();
            res = this_res;
            break;
        }

        let sleep = time::sleep(Duration::from_secs(1));
        timeout_at(deadline, sleep).await.unwrap();
    }

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
    }
}
