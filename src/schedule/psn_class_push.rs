use std::sync::Arc;

use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper, QueryType};
use crate::schedule::BasePsnPushTask;
use crate::{AppContext, ClassData, DynamicPsnData, PsnDataKind, TaskExecutor};
use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

// 具体的任务结构体只包含 BasePsnPushTask 和它特有的数据
pub struct PsnClassPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnClassPushTask {
    type DataType = ClassData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Class(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/classes.sql");
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        match query_type {
            QueryType::ByDate(hit_date) => {
                query_builder.push(" AND a.hitdate = ");
                query_builder.push_bind(hit_date);
            }
            QueryType::ByIds(ids) => {
                query_builder.push(" AND a.TRAINID IN (");
                let mut separated = query_builder.separated(", ");
                for id in ids {
                    separated.push_bind(id);
                }
                separated.push_unseparated(")");
            }
        }
        query_builder
    }

    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::Class
    }
}

impl PsnClassPushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        Self {
            base: BasePsnPushTask::new(app_context, hit_date, train_ids),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for PsnClassPushTask {
    async fn execute(&self) -> Result<()> {
        execute_push_task_logic::<PsnClassPushTask>(&self.base).await
    }
}
