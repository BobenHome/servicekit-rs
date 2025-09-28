use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

use crate::models::train::ArchiveData;
use crate::schedule::BasePsnPushTask;
use crate::schedule::push_executor::{PsnDataWrapper, QueryType, execute_push_task_logic};
use crate::{AppContext, DynamicPsnData, PsnDataKind, TaskExecutor};

pub struct PsnArchivePushTask {
    base: BasePsnPushTask,
}

impl PsnDataWrapper for PsnArchivePushTask {
    type DataType = ArchiveData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Archive(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/archive.sql");
        // 使用 QueryBuilder 创建查询构建器
        let query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        Self::apply_query_filters(query_builder, query_type, "c.hitdate", "c.TRAINID")
    }

    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::Archive
    }
}

impl PsnArchivePushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnArchivePushTask {
            base: BasePsnPushTask::new(app_context, hit_date, train_ids),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for PsnArchivePushTask {
    async fn execute(&self) -> Result<()> {
        execute_push_task_logic::<PsnArchivePushTask>(&self.base).await
    }
}
