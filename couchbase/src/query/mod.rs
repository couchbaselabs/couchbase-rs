//! Contains query handling routines (n1ql, views, fts,...)
pub mod n1ql;
pub mod views;

pub use self::n1ql::{N1qlMeta, N1qlRow, N1qlResult};
pub use self::views::{ViewMeta, ViewRow, ViewResult};
