use chrono::{DateTime, FixedOffset};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TermFacet {
    #[builder(!default)]
    pub field: String,
    pub size: Option<u64>,
}

impl From<TermFacet> for couchbase_core::searchx::facets::TermFacet {
    fn from(facet: TermFacet) -> Self {
        couchbase_core::searchx::facets::TermFacet::builder()
            .field(facet.field)
            .size(facet.size)
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct NumericRange {
    #[builder(!default)]
    pub name: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl From<NumericRange> for couchbase_core::searchx::facets::NumericRange {
    fn from(facet: NumericRange) -> Self {
        couchbase_core::searchx::facets::NumericRange::builder()
            .name(facet.name)
            .min(facet.min)
            .max(facet.max)
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)),mutators(
        pub fn add_numeric_range(&mut self, range: NumericRange) {
            self.numeric_ranges.push(range);
        }
))]
#[non_exhaustive]
pub struct NumericRangeFacet {
    #[builder(!default)]
    pub field: String,
    pub size: Option<u64>,
    #[builder(via_mutators)]
    pub numeric_ranges: Vec<NumericRange>,
}

impl From<NumericRangeFacet> for couchbase_core::searchx::facets::NumericRangeFacet {
    fn from(facet: NumericRangeFacet) -> Self {
        couchbase_core::searchx::facets::NumericRangeFacet::builder()
            .field(facet.field)
            .size(facet.size)
            .numeric_ranges(
                facet
                    .numeric_ranges
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<couchbase_core::searchx::facets::NumericRange>>(),
            )
            .build()
    }
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

impl From<DateRange> for couchbase_core::searchx::facets::DateRange {
    fn from(facet: DateRange) -> Self {
        couchbase_core::searchx::facets::DateRange::builder()
            .name(facet.name)
            .start(facet.start)
            .end(facet.end)
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)), mutators(
        pub fn add_date_range(&mut self, range: DateRange) {
            self.date_ranges.push(range);
        }
))]
#[non_exhaustive]
pub struct DateRangeFacet {
    #[builder(!default)]
    pub field: String,
    pub size: Option<u64>,
    #[builder(via_mutators)]
    pub date_ranges: Vec<DateRange>,
}

impl From<DateRangeFacet> for couchbase_core::searchx::facets::DateRangeFacet {
    fn from(facet: DateRangeFacet) -> Self {
        couchbase_core::searchx::facets::DateRangeFacet::builder()
            .field(facet.field)
            .size(facet.size)
            .date_ranges(
                facet
                    .date_ranges
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<couchbase_core::searchx::facets::DateRange>>(),
            )
            .build()
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
