mod codec;
mod connection;
mod generated;
mod insert;
mod literalize;
mod scan;
mod schema;
mod table;
mod transform;
mod utils;

pub use codec::*;
pub use connection::*;
pub use insert::*;
pub use literalize::*;
pub use scan::*;
pub use schema::*;
pub use table::*;
pub use transform::*;
pub use utils::*;

pub(crate) type DFResult<T> = datafusion_common::Result<T>;

#[cfg(not(any(
    feature = "mysql",
    feature = "postgres",
    feature = "oracle",
    feature = "sqlite",
    feature = "dm",
)))]
compile_error!(
    "At least one of the following features must be enabled: postgres, mysql, oracle, sqlite, dm"
);
