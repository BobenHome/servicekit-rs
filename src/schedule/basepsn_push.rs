use std::sync::Arc;

use crate::config::MssInfoConfig;
use crate::mappers::archiving_mss_mapper::ArchivingMssMapper;
use crate::parsers::push_result_parser::PushResultParser;
use crate::utils::GatewayClient;
use reqwest::Client;
use sqlx::MySqlPool;

// 封装所有任务共享的字段
pub struct BasePsnPushTask {
    pub pool: MySqlPool,
    pub http_client: Client, // 更改为 pub，方便从外部访问，如果需要
    pub mss_info_config: MssInfoConfig,
    pub archiving_mapper: ArchivingMssMapper,
    pub push_result_parser: PushResultParser,
    pub task_name: String, // 每个具体任务的名称
    pub gateway_client: Arc<GatewayClient>,
}

impl BasePsnPushTask {
    pub fn new(
        pool: MySqlPool,
        config: MssInfoConfig,
        task_name: String,
        gateway_client: Arc<GatewayClient>,
    ) -> Self {
        // MySqlPool 是 Arc 包装的，所以可以安全克隆
        let pool_clone_for_mapper = pool.clone();
        let pool_clone_for_parser = pool.clone();

        BasePsnPushTask {
            http_client: Client::new(),
            mss_info_config: config,
            archiving_mapper: ArchivingMssMapper::new(pool_clone_for_mapper),
            push_result_parser: PushResultParser::new(pool_clone_for_parser),
            pool, // 自身也需要持有 pool
            task_name,
            gateway_client,
        }
    }
}
