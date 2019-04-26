use crate::*;
use connector::{error::ConnectorError, mutaction::*, ConnectorResult, DatabaseMutactionExecutor};
use serde_json::Value;
use std::sync::Arc;

impl DatabaseMutactionExecutor for Sqlite {
    fn execute_raw(&self, _query: String) -> ConnectorResult<Value> {
        // self.sqlite.with_connection(&db_name, |conn| {
        //     let res = conn
        //         .prepare(&query)?
        //         .query_map(&params, |row| f(row))?
        //         .map(|row_res| row_res.unwrap())
        //         .collect();

        //     Ok(res)
        // });
        Ok(Value::String("hello world!".to_string()))
    }

    fn execute(
        &self,
        db_name: String,
        mutaction: TopLevelDatabaseMutaction,
    ) -> ConnectorResult<DatabaseMutactionResult> {
        self.with_transaction(&db_name, |conn| {
            fn create(conn: &mut Transaction, cn: &CreateNode) -> ConnectorResult<DatabaseMutactionResult> {
                let parent_id = Sqlite::execute_create(conn, Arc::clone(&cn.model), &cn.non_list_args, &cn.list_args)?;
                Sqlite::execute_nested(conn, &cn.nested_mutactions, &parent_id)?;

                Ok(DatabaseMutactionResult {
                    identifier: Identifier::Id(parent_id),
                    typ: DatabaseMutactionResultType::Create,
                })
            }

            fn update(conn: &mut Transaction, un: &UpdateNode) -> ConnectorResult<DatabaseMutactionResult> {
                let parent_id = Sqlite::execute_update(conn, &un.where_, &un.non_list_args, &un.list_args)?;
                Sqlite::execute_nested(conn, &un.nested_mutactions, &parent_id)?;

                Ok(DatabaseMutactionResult {
                    identifier: Identifier::Id(parent_id),
                    typ: DatabaseMutactionResultType::Update,
                })
            }

            match mutaction {
                TopLevelDatabaseMutaction::CreateNode(ref cn) => create(conn, cn),
                TopLevelDatabaseMutaction::UpdateNode(ref un) => update(conn, un),
                TopLevelDatabaseMutaction::UpsertNode(ref ups) => match conn.find_id(&ups.where_) {
                    Err(_e @ ConnectorError::NodeNotFoundForWhere { .. }) => create(conn, &ups.create),
                    Err(e) => return Err(e),
                    Ok(_) => update(conn, &ups.update),
                },
                TopLevelDatabaseMutaction::UpdateNodes(ref uns) => {
                    let count = Self::execute_update_many(
                        conn,
                        Arc::clone(&uns.model),
                        &uns.filter,
                        &uns.non_list_args,
                        &uns.list_args,
                    )?;

                    Ok(DatabaseMutactionResult {
                        identifier: Identifier::Count(count),
                        typ: DatabaseMutactionResultType::Many,
                    })
                }
                TopLevelDatabaseMutaction::DeleteNode(ref dn) => {
                    let node = Self::execute_delete(conn, &dn.where_)?;

                    Ok(DatabaseMutactionResult {
                        identifier: Identifier::Node(node),
                        typ: DatabaseMutactionResultType::Delete,
                    })
                }
                TopLevelDatabaseMutaction::DeleteNodes(ref dns) => {
                    let count = Self::execute_delete_many(conn, Arc::clone(&dns.model), &dns.filter)?;

                    Ok(DatabaseMutactionResult {
                        identifier: Identifier::Count(count),
                        typ: DatabaseMutactionResultType::Many,
                    })
                }
                TopLevelDatabaseMutaction::ResetData(ref rd) => {
                    conn.truncate(Arc::clone(&rd.project))?;

                    Ok(DatabaseMutactionResult {
                        identifier: Identifier::None,
                        typ: DatabaseMutactionResultType::Unit,
                    })
                }
            }
        })
    }
}
