use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};

use crate::models::train::ArchiveData;
use crate::schedule::push_executor::{execute_push_task_logic, PsnDataWrapper, QueryType};
use crate::schedule::BasePsnPushTask;
use crate::utils::{ClickHouseClient, GatewayClient};
use crate::{DynamicPsnData, MssInfoConfig, PsnDataKind, TaskExecutor};

pub struct PsnArchiveScPushTask {
    base: BasePsnPushTask, // <-- 嵌入 BasePsnPushTask
}

impl PsnDataWrapper for PsnArchiveScPushTask {
    type DataType = ArchiveData;
    fn wrap_data(data: Self::DataType) -> DynamicPsnData {
        DynamicPsnData::Archive(data)
    }

    fn get_query_builder(query_type: QueryType) -> QueryBuilder<'static, MySql> {
        // <-- 显式地将 sqlx::query_file! 的结果存入变量，再调用 .sql()
        let raw_sql_query = sqlx::query_file!("queries/archive_sc.sql");
        // 使用 QueryBuilder 创建查询构建器
        let mut query_builder = QueryBuilder::<MySql>::new(raw_sql_query.sql());

        match query_type {
            QueryType::ByDate(hit_date) => {
                query_builder.push(" AND c.hitdate = ");
                query_builder.push_bind(hit_date);
            }
            QueryType::ByIds(ids) => {
                query_builder.push(" AND c.TRAINID IN (");
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
        PsnDataKind::ArchiveSc
    }
}

impl PsnArchiveScPushTask {
    pub fn new(
        pool: MySqlPool,
        config: Arc<MssInfoConfig>,
        gateway_client: Arc<GatewayClient>,
        clickhouse_client: Arc<ClickHouseClient>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        PsnArchiveScPushTask {
            base: BasePsnPushTask::new(
                pool,
                config,
                gateway_client,
                clickhouse_client,
                hit_date,
                train_ids,
            ),
        }
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnArchiveScPushTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(execute_push_task_logic::<PsnArchiveScPushTask>(&self.base))
    }
}
