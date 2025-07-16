use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};

use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper};
use crate::schedule::BasePsnPushTask;
use crate::utils::GatewayClient;
use crate::{LecturerData, MssInfoConfig, TaskExecutor};

pub struct PsnLecturerPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnLecturerPushTask {
    type DataType = LecturerData;
    fn wrap_data(data: Self::DataType) -> crate::DynamicPsnData {
        crate::DynamicPsnData::Lecturer(data)
    }
    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql> {
        let raw_sql_query = sqlx::query_file!("queries/lecturers.sql");
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());
        query_builder.push(" AND T.hitdate = ");
        query_builder.push_bind(hit_date.to_string());
        query_builder.push(" LIMIT 1 ");
        query_builder
    }
}

impl PsnLecturerPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig, gateway_client: Arc<GatewayClient>) -> Self {
        PsnLecturerPushTask {
            base: BasePsnPushTask::new(
                pool,
                config,
                "PsnLecturerPushTask".to_string(),
                gateway_client,
            ),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnLecturerPushTask {
    fn name(&self) -> &str {
        self.base.task_name.as_str()
    }
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnLecturerPushTask>(&self.base))
    }
}
