use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper, QueryType};
use crate::schedule::BasePsnPushTask;
use crate::{AppContext, ClassData, DynamicPsnData, PsnDataKind, TaskExecutor};
use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

// 具体的任务结构体只包含 BasePsnPushTask 和它特有的数据
pub struct PsnTrainScPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnTrainScPushTask {
    type DataType = ClassData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Class(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/trains_sc.sql");
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
        PsnDataKind::ClassSc
    }
}

impl PsnTrainScPushTask {
    pub fn new(
        context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnTrainScPushTask {
            base: BasePsnPushTask::new(
                context.pool.clone(),
                Arc::clone(&context.mss_info_config),
                Arc::clone(&context.gateway_client),
                Arc::clone(&context.clickhouse_client),
                hit_date,
                train_ids,
            ),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnTrainScPushTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnTrainScPushTask>(&self.base))
    }
}
