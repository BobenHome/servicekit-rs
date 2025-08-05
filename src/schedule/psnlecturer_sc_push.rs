use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper, QueryType};
use crate::schedule::BasePsnPushTask;
use crate::{AppContext, LecturerData, PsnDataKind, TaskExecutor};

pub struct PsnLecturerScPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnLecturerScPushTask {
    type DataType = LecturerData;
    fn wrap_data(data: Self::DataType) -> crate::DynamicPsnData {
        crate::DynamicPsnData::Lecturer(data)
    }
    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        let raw_sql_query = sqlx::query_file!("queries/lecturers_sc.sql");
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        match query_type {
            QueryType::ByDate(hit_date) => {
                query_builder.push(" AND T.hitdate = ");
                query_builder.push_bind(hit_date);
            }
            QueryType::ByIds(ids) => {
                query_builder.push(" AND T.TRAINID IN (");
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
        PsnDataKind::LecturerSc
    }
}

impl PsnLecturerScPushTask {
    pub fn new(
        context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnLecturerScPushTask {
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
impl TaskExecutor for PsnLecturerScPushTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnLecturerScPushTask>(&self.base))
    }
}
