use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};

use crate::models::train::ArchiveData;
use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper};
use crate::schedule::BasePsnPushTask;
use crate::utils::{ClickHouseClient, GatewayClient};
use crate::{DynamicPsnData, MssInfoConfig, PsnDataKind, TaskExecutor};

pub struct PsnArchivePushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnArchivePushTask {
    type DataType = ArchiveData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Archive(data)
    }

    fn get_final_query_builder(hit_date: &str) -> QueryBuilder<'static, MySql> {
        // <-- 修正：显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/archive.sql");
        // 使用 QueryBuilder 创建查询构建器
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());
        query_builder.push(" AND c.hitdate = ");
        query_builder.push_bind(hit_date.to_string());
        query_builder.push(" LIMIT 1 ");
        query_builder
    }

    fn get_psn_data_kind_for_wrapper() -> PsnDataKind {
        PsnDataKind::Archive
    }
}

impl PsnArchivePushTask {
    pub fn new(
        pool: MySqlPool,
        config: MssInfoConfig,
        gateway_client: Arc<GatewayClient>,
        clickhouse_client: Arc<ClickHouseClient>,
    ) -> Self {
        PsnArchivePushTask {
            base: BasePsnPushTask::new(pool, config, gateway_client, clickhouse_client),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnArchivePushTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnArchivePushTask>(&self.base))
    }
}
