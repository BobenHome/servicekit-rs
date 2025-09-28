use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

use crate::schedule::BasePsnPushTask;
use crate::schedule::push_executor::{PsnDataWrapper, QueryType, execute_push_task_logic};
use crate::{AppContext, LecturerData, PsnDataKind, TaskExecutor};

pub struct PsnLecturerPushTask {
    base: BasePsnPushTask,
}

impl PsnDataWrapper for PsnLecturerPushTask {
    type DataType = LecturerData;
    fn wrap_data(data: Self::DataType) -> crate::DynamicPsnData {
        crate::DynamicPsnData::Lecturer(data)
    }
    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        let raw_sql_query = sqlx::query_file!("queries/lecturers.sql");
        let query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        Self::apply_query_filters(query_builder, query_type, "T.hitdate", "T.TRAINID")
    }
    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::Lecturer
    }
}

impl PsnLecturerPushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnLecturerPushTask {
            base: BasePsnPushTask::new(app_context, hit_date, train_ids),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for PsnLecturerPushTask {
    async fn execute(&self) -> Result<()> {
        execute_push_task_logic::<PsnLecturerPushTask>(&self.base).await
    }
}
