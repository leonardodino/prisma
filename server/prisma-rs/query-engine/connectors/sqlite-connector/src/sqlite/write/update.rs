use crate::{mutaction::MutationBuilder, DatabaseUpdate, Sqlite, Transaction};
use connector::{
    filter::{Filter, NodeSelector},
    ConnectorResult,
};
use prisma_models::{GraphqlId, ModelRef, PrismaArgs, PrismaListValue, RelationFieldRef};
use std::sync::Arc;

impl DatabaseUpdate for Sqlite {
    fn execute_update<T>(
        conn: &mut Transaction,
        node_selector: &NodeSelector,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<GraphqlId>
    where
        T: AsRef<str>,
    {
        let model = node_selector.field.model();
        let id = conn.find_id(node_selector)?;

        if let Some(update) = MutationBuilder::update_one(Arc::clone(&model), &id, non_list_args)? {
            conn.update(update)?;
        }

        Self::update_list_args(conn, &[id.clone()], Arc::clone(&model), list_args)?;

        Ok(id)
    }

    fn execute_update_many<T>(
        conn: &mut Transaction,
        model: ModelRef,
        filter: &Filter,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<usize>
    where
        T: AsRef<str>,
    {
        let ids = conn.filter_ids(Arc::clone(&model), filter.clone())?;
        let count = ids.len();

        let updates = {
            let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
            MutationBuilder::update_many(Arc::clone(&model), ids.as_slice(), non_list_args)?
        };

        for update in updates {
            conn.update(update)?;
        }

        Self::update_list_args(conn, ids.as_slice(), Arc::clone(&model), list_args)?;

        Ok(count)
    }

    fn execute_nested_update<T>(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        node_selector: &Option<NodeSelector>,
        relation_field: RelationFieldRef,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<GraphqlId>
    where
        T: AsRef<str>,
    {
        if let Some(ref node_selector) = node_selector {
            conn.find_id(node_selector)?;
        };

        let id = conn.find_id_by_parent(Arc::clone(&relation_field), parent_id, node_selector)?;
        let node_selector = NodeSelector::from((relation_field.related_model().fields().id(), id));

        Self::execute_update(conn, &node_selector, non_list_args, list_args)
    }

    fn execute_nested_update_many<T>(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        filter: &Option<Filter>,
        relation_field: RelationFieldRef,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<usize>
    where
        T: AsRef<str>,
    {
        let ids = conn.filter_ids_by_parents(Arc::clone(&relation_field), vec![parent_id], filter.clone())?;
        let count = ids.len();

        let updates = {
            let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
            MutationBuilder::update_many(relation_field.related_model(), ids.as_slice(), non_list_args)?
        };

        for update in updates {
            conn.update(update)?;
        }

        Self::update_list_args(conn, ids.as_slice(), relation_field.model(), list_args)?;

        Ok(count)
    }

    fn update_list_args<T>(
        conn: &mut Transaction,
        ids: &[GraphqlId],
        model: ModelRef,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<()>
    where
        T: AsRef<str>,
    {
        for (field_name, list_value) in list_args {
            let field = model.fields().find_from_scalar(field_name.as_ref()).unwrap();
            let table = field.scalar_list_table();
            let (deletes, inserts) = MutationBuilder::update_scalar_list_values(&table, &list_value, ids.to_vec());

            for delete in deletes {
                conn.delete(delete)?;
            }

            for insert in inserts {
                conn.insert(insert)?;
            }
        }

        Ok(())
    }
}
