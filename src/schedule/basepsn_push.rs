use std::sync::Arc;
use std::time::Duration;

use crate::config::MssInfoConfig;
use crate::mappers::archiving_mss_mapper::ArchivingMssMapper;
use crate::parsers::push_result_parser::PushResultParser;
use crate::utils::{ClickHouseClient, GatewayClient};
use reqwest::Client;
use sqlx::MySqlPool;

// 封装所有任务共享的字段
pub struct BasePsnPushTask {
    pub pool: MySqlPool,
    pub http_client: Client, // 更改为 pub，方便从外部访问，如果需要
    pub mss_info_config: MssInfoConfig,
    pub archiving_mapper: ArchivingMssMapper,
    pub push_result_parser: PushResultParser,
    pub gateway_client: Arc<GatewayClient>,
    pub clickhouse_client: Arc<ClickHouseClient>, // 添加 ClickHouse 客户端
    pub hit_date: Option<String>,                 // 存储可选的 hit_date
    pub train_ids: Option<Vec<String>>,           // 存储可选的 train_ids
}

impl BasePsnPushTask {
    pub fn new(
        pool: MySqlPool,
        config: MssInfoConfig,
        gateway_client: Arc<GatewayClient>,
        clickhouse_client: Arc<ClickHouseClient>,
        hit_date: Option<String>,
        train_ids: Option<Vec<String>>,
    ) -> Self {
        // MySqlPool 是 Arc 包装的，所以可以安全克隆
        let pool_clone_for_mapper = pool.clone();
        let pool_clone_for_parser = pool.clone();

        // 自定义 HTTP 客户端，设置超时
        let http_client = Client::builder()
            .connect_timeout(Duration::from_secs(5)) // TCP连接最多等5秒
            .read_timeout(Duration::from_secs(5)) // 读取响应最多等5秒
            .timeout(Duration::from_secs(10)) // 整个请求最多10秒
            .build()
            .expect("Failed to build reqwest client");

        BasePsnPushTask {
            http_client,
            mss_info_config: config,
            archiving_mapper: ArchivingMssMapper::new(pool_clone_for_mapper),
            push_result_parser: PushResultParser::new(pool_clone_for_parser),
            pool,
            gateway_client,
            clickhouse_client,
            hit_date,
            train_ids,
        }
    }
}
