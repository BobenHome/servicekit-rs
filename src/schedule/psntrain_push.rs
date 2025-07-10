use std::future::Future;
use std::pin::Pin;

use crate::config::MssInfoConfig;
use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper};
use crate::schedule::BasePsnPushTask;
use crate::{ClassData, DynamicPsnData, TaskExecutor};
use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};

// 具体的任务结构体只包含 BasePsnPushTask 和它特有的数据
pub struct PsnTrainPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnTrainPushTask {
    type DataType = ClassData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Class(data)
    }

    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/trains.sql");
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());
        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(hit_date.to_string());
        query_builder.push(" LIMIT 1 ");
        query_builder
    }
}

impl PsnTrainPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig) -> Self {
        PsnTrainPushTask {
            base: BasePsnPushTask::new(pool, config, "PsnTrainPushTask".to_string()),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnTrainPushTask {
    fn name(&self) -> &str {
        self.base.task_name.as_str()
    }
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnTrainPushTask>(&self.base))
    }
}
