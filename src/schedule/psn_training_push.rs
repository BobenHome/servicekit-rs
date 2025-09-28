use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

use crate::models::train::TrainingData;
use crate::schedule::BasePsnPushTask;
use crate::schedule::push_executor::{PsnDataWrapper, QueryType, execute_push_task_logic};
use crate::{AppContext, DynamicPsnData, PsnDataKind, TaskExecutor};

pub struct PsnTrainingPushTask {
    base: BasePsnPushTask,
}

impl PsnDataWrapper for PsnTrainingPushTask {
    type DataType = TrainingData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Training(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/trainings.sql");
        let query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        Self::apply_query_filters(query_builder, query_type, "c.hitdate", "c.TRAINID")
    }

    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::Training
    }
}

impl PsnTrainingPushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnTrainingPushTask {
            base: BasePsnPushTask::new(app_context, hit_date, train_ids),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for PsnTrainingPushTask {
    async fn execute(&self) -> Result<()> {
        execute_push_task_logic::<PsnTrainingPushTask>(&self.base).await
    }
}
