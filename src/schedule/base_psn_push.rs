use std::sync::Arc;

use crate::config::MssInfoConfig;
use crate::mappers::archiving_mss_mapper::ArchivingMssMapper;
use crate::parsers::push_result_parser::PushResultParser;
use crate::utils::{ClickHouseClient, GatewayClient};
use crate::AppContext;
use reqwest::Client;
use sqlx::MySqlPool;

// 封装所有任务共享的字段
pub struct BasePsnPushTask {
    pub mysql_pool: MySqlPool,
    pub http_client: Client,
    pub mss_info_config: Arc<MssInfoConfig>,
    pub archiving_mapper: ArchivingMssMapper,
    pub push_result_parser: PushResultParser,
    pub gateway_client: Arc<GatewayClient>,
    pub clickhouse_client: Arc<ClickHouseClient>, // 添加 ClickHouse 客户端
    pub hit_date: Option<String>,                 // 存储可选的 hit_date
    pub train_ids: Option<Vec<String>>,           // 存储可选的 train_ids
}

impl BasePsnPushTask {
    pub fn new(
        app_context: Arc<AppContext>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        // MySqlPool 是 Arc 包装的，所以可以安全克隆
        let pool_clone_for_mapper = app_context.mysql_pool.clone();
        let pool_clone_for_parser = app_context.mysql_pool.clone();

        BasePsnPushTask {
            mysql_pool: app_context.mysql_pool.clone(),
            http_client: app_context.http_client.clone(),
            mss_info_config: Arc::clone(&app_context.mss_info_config),
            archiving_mapper: ArchivingMssMapper::new(pool_clone_for_mapper),
            push_result_parser: PushResultParser::new(pool_clone_for_parser),
            gateway_client: Arc::clone(&app_context.gateway_client),
            clickhouse_client: Arc::clone(&app_context.clickhouse_client),
            hit_date,
            train_ids,
        }
    }
}
