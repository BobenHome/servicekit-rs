use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{MssInfoConfig, RedisConfig, TelecomConfig};
use crate::db::mysql_pool;
use crate::utils::redis::{init_redis, RedisMgr};
use crate::utils::{ClickHouseClient, GatewayClient};
use crate::ClickhouseConfig;
use anyhow::{Context as _, Result};
use reqwest::Client;
use sqlx::MySqlPool;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct AppContext {
    pub mysql_pool: MySqlPool,
    pub http_client: Client,
    pub mss_info_config: Arc<MssInfoConfig>,
    pub gateway_client: Arc<GatewayClient>,
    pub clickhouse_client: Arc<ClickHouseClient>,
    pub redis_mgr: RedisMgr,
    pub provinces: Arc<HashMap<String, String>>,
}

impl AppContext {
    pub async fn new(
        database_url: &str,
        mss_info_config: Arc<MssInfoConfig>,
        telecom_config: Arc<TelecomConfig>,
        clickhouse_config: Arc<ClickhouseConfig>,
        redis_config: Arc<RedisConfig>,
        provinces: HashMap<String, String>,
    ) -> Result<Self> {
        // --- Initialize MYSQL POOL ---
        let mysql_pool = mysql_pool::create_mysql_pool(database_url)
            .await
            .context("Failed to create database connection mysql_pool")?;
        info!("Database connection mysql_pool created.");

        // --- Initialize HTTP ---
        // 自定义 HTTP 客户端，设置超时
        let http_client = Client::builder()
            .connect_timeout(Duration::from_secs(5)) // TCP连接最多等5秒
            .read_timeout(Duration::from_secs(5)) // 读取响应最多等5秒
            .timeout(Duration::from_secs(10)) // 整个请求最多10秒
            .build()
            .expect("Failed to build reqwest client");
        info!("HTTP Client initialized.");

        // --- Initialize GatewayClient ---
        let gateway_client = Arc::new(GatewayClient::new(http_client.clone(), telecom_config));
        info!("GatewayClient initialized.");

        // --- Initialize ClickHouseClient ---
        let clickhouse_client = Arc::new(
            ClickHouseClient::new(clickhouse_config)
                .context("Failed to initialize ClickHouseClient")?,
        );
        info!("ClickHouseClient initialized.");

        let redis_mgr: RedisMgr = init_redis(&redis_config.url)
            .await
            .context("Failed to initialize Redis ConnectionManager")?;

        info!("Redis ConnectionManager initialized.");
        Ok(Self {
            mysql_pool,
            http_client,
            mss_info_config,
            gateway_client,
            clickhouse_client,
            redis_mgr,
            provinces: Arc::new(provinces),
        })
    }
}

#[derive(Clone)]
pub struct RedisContext {
    pub redis_mgr: RedisMgr,
}

impl RedisContext {
    pub async fn new(redis_config: Arc<RedisConfig>) -> Result<Self> {
        let redis_mgr: RedisMgr = init_redis(&redis_config.url)
            .await
            .context("Failed to initialize Redis ConnectionManager")?;
        info!("Redis ConnectionManager initialized.");
        Ok(Self { redis_mgr })
    }
}
