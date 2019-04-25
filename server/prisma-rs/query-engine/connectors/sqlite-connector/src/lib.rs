//! # The SQL Connector interface
//!
//! The public interface to outside is split into separate traits:
//!
//! - [DataResolver](../connector/trait.DataResolver.html) to fetch data.
//! - [DatabaseMutactionExecutor](../connector/trait.DatabaseMutactionExecutor.html) to write
//!   data.
//!
//! Additionally the connector splits internal logic into separate traits:
//!
//! - [DatabaseRead](trait.DatabaseRead.html) for reading from the database.
//! - [DatabaseWrite](trait.DatabaseWrite.html) for writing to the database.
//! - [TransactionalExecutor](trait.TransactionalExecutor.html) for abstract transaction and connection handling.
//!
//! The implementation is currently only for [SQLite](struct.Sqlite.html).

mod cursor_condition;
mod database_write;
mod filter_conversion;
mod mutaction;
mod ordering;
mod query_builder;
mod row;
mod sqlite;
mod transaction;

pub use database_write::*;
pub use filter_conversion::*;
pub use mutaction::*;
pub use query_builder::SelectDefinition;
pub use row::*;
pub use sqlite::*;
pub use transaction::*;
