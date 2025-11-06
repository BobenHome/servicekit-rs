use std::sync::Arc;

use crate::schedule::BasePsnPushTask;
use crate::schedule::push_executor::{PsnDataWrapper, QueryType, execute_push_task_logic};
use crate::{AppContext, ClassData, DynamicPsnData, PsnDataKind, TaskExecutor};
use anyhow::Result;
use sqlx::{Execute, MySql, QueryBuilder};

// 具体的任务结构体只包含 BasePsnPushTask 和它特有的数据
pub struct PsnClassScPushTask {
    base: BasePsnPushTask,
}

impl PsnDataWrapper for PsnClassScPushTask {
    type DataType = ClassData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Class(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/classes_sc.sql");
        let query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        Self::apply_query_filters(query_builder, query_type, "a.hitdate", "a.TRAINID")
    }

    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::ClassSc
    }
}

impl PsnClassScPushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnClassScPushTask {
            base: BasePsnPushTask::new(app_context, hit_date, train_ids),
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for PsnClassScPushTask {
    async fn execute(&self) -> Result<()> {
        execute_push_task_logic::<PsnClassScPushTask>(&self.base).await
    }
}
