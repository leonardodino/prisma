use crate::{
    mutaction::{MutationBuilder, NestedActions},
    DatabaseRelation, Sqlite, Transaction,
};
use connector::{filter::NodeSelector, ConnectorResult};
use prisma_models::{GraphqlId, RelationFieldRef};
use std::sync::Arc;

impl DatabaseRelation for Sqlite {
    fn execute_connect(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        actions: &NestedActions,
        node_selector: &NodeSelector,
        relation_field: RelationFieldRef,
    ) -> ConnectorResult<()> {
        if let Some((select, check)) = actions.required_check(parent_id)? {
            let ids = conn.select_ids(select)?;
            check.call_box(ids.into_iter().next().is_some())?
        }

        let child_id = conn.find_id(node_selector)?;

        if let Some(query) = actions.parent_removal(parent_id) {
            conn.write(query)?;
        }

        if let Some(query) = actions.child_removal(&child_id) {
            conn.write(query)?;
        }

        let relation_query = MutationBuilder::create_relation(relation_field, parent_id, &child_id);
        conn.write(relation_query)?;

        Ok(())
    }

    fn execute_disconnect(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        actions: &NestedActions,
        node_selector: &Option<NodeSelector>,
    ) -> ConnectorResult<()> {
        if let Some((select, check)) = actions.required_check(parent_id)? {
            let ids = conn.select_ids(select)?;
            check.call_box(ids.into_iter().next().is_some())?
        }

        match node_selector {
            None => {
                let (select, check) = actions.ensure_parent_is_connected(parent_id);

                let ids = conn.select_ids(select)?;
                check.call_box(ids.into_iter().next().is_some())?;

                conn.write(actions.removal_by_parent(parent_id))?;
            }
            Some(ref selector) => {
                let child_id = conn.find_id(selector)?;
                let (select, check) = actions.ensure_connected(parent_id, &child_id);

                let ids = conn.select_ids(select)?;
                check.call_box(ids.into_iter().next().is_some())?;

                conn.write(actions.removal_by_parent_and_child(parent_id, &child_id))?;
            }
        }

        Ok(())
    }

    fn execute_set(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        actions: &NestedActions,
        node_selectors: &Vec<NodeSelector>,
        relation_field: RelationFieldRef,
    ) -> ConnectorResult<()> {
        if let Some((select, check)) = actions.required_check(parent_id)? {
            let ids = conn.select_ids(select)?;
            check.call_box(ids.into_iter().next().is_some())?
        }

        conn.write(actions.removal_by_parent(parent_id))?;

        for selector in node_selectors {
            let child_id = conn.find_id(selector)?;

            if !relation_field.is_list {
                conn.write(actions.removal_by_child(&child_id))?;
            }

            let relation_query = MutationBuilder::create_relation(Arc::clone(&relation_field), parent_id, &child_id);
            conn.write(relation_query)?;
        }

        Ok(())
    }
}
