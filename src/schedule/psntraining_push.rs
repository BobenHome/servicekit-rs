use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};

use crate::models::train::TrainingData;
use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper};
use crate::schedule::BasePsnPushTask;
use crate::utils::GatewayClient;
use crate::{DynamicPsnData, MssInfoConfig, TaskExecutor};

pub struct PsnTrainingPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnTrainingPushTask {
    type DataType = TrainingData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Training(data)
    }

    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql> {
        // <-- 修正：显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/trainings.sql");
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());
        query_builder.push(" AND c.hitdate = ");
        query_builder.push_bind(hit_date.to_string());
        query_builder.push(" LIMIT 1 ");
        query_builder
    }
}

impl PsnTrainingPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig, gateway_client: Arc<GatewayClient>) -> Self {
        PsnTrainingPushTask {
            base: BasePsnPushTask::new(
                pool,
                config,
                "PsnTrainingPushTask".to_string(),
                gateway_client,
            ),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnTrainingPushTask {
    fn name(&self) -> &str {
        self.base.task_name.as_str()
    }
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnTrainingPushTask>(&self.base))
    }
}
