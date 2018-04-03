//! Contains query handling routines (n1ql, views, fts,...)
pub mod n1ql;
pub mod views;

pub use self::n1ql::{N1qlMeta, N1qlResult, N1qlRow};
pub use self::views::{ViewMeta, ViewResult, ViewRow};
