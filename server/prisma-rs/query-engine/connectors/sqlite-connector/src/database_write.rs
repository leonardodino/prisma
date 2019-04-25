mod create;
mod delete;
mod relation;
mod update;

pub use create::*;
pub use delete::*;
pub use relation::*;
pub use update::*;

use crate::Transaction;
use connector::{mutaction::NestedMutactions, ConnectorResult};
use prisma_models::GraphqlId;

pub trait DatabaseWrite {
    fn execute_nested(
        conn: &mut Transaction,
        mutaction: &NestedMutactions,
        parent_id: &GraphqlId,
    ) -> ConnectorResult<()>;
}
