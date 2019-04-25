use crate::{
    mutaction::{MutationBuilder, NestedActions},
    DatabaseCreate, Sqlite, Transaction,
};
use connector::ConnectorResult;
use prisma_models::{GraphqlId, ModelRef, PrismaArgs, PrismaListValue, RelationFieldRef};
use std::sync::Arc;

impl DatabaseCreate for Sqlite {
    fn execute_create<T>(
        conn: &mut Transaction,
        model: ModelRef,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<GraphqlId>
    where
        T: AsRef<str>,
    {
        let (insert, returned_id) = MutationBuilder::create_node(Arc::clone(&model), non_list_args.clone());
        let last_id = conn.insert(insert)?;

        let id = match returned_id {
            Some(id) => id,
            None => GraphqlId::Int(last_id),
        };

        for (field_name, list_value) in list_args {
            let field = model.fields().find_from_scalar(field_name.as_ref()).unwrap();
            let table = field.scalar_list_table();

            if let Some(insert) = MutationBuilder::create_scalar_list_value(table.table(), &list_value, &id) {
                conn.insert(insert)?;
            }
        }

        Ok(id)
    }

    fn execute_nested_create<T>(
        conn: &mut Transaction,
        parent_id: &GraphqlId,
        actions: &NestedActions,
        relation_field: RelationFieldRef,
        non_list_args: &PrismaArgs,
        list_args: &[(T, PrismaListValue)],
    ) -> ConnectorResult<GraphqlId>
    where
        T: AsRef<str>,
    {
        if let Some((select, check)) = actions.required_check(parent_id)? {
            let ids = conn.select_ids(select)?;
            check.call_box(ids.into_iter().next().is_some())?
        };

        if let Some(query) = actions.parent_removal(parent_id) {
            conn.write(query)?;
        }

        let related_field = relation_field.related_field();

        if related_field.relation_is_inlined_in_parent() {
            let mut prisma_args = non_list_args.clone();
            prisma_args.insert(related_field.name.clone(), parent_id.clone());

            Self::execute_create(conn, relation_field.related_model(), &prisma_args, list_args)
        } else {
            let id = Self::execute_create(conn, relation_field.related_model(), non_list_args, list_args)?;
            let relation_query = MutationBuilder::create_relation(relation_field, parent_id, &id);

            conn.write(relation_query)?;

            Ok(id)
        }
    }
}
