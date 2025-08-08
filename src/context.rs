use std::sync::Arc;

use crate::config::{MssInfoConfig, TelecomConfig};
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
}

impl AppContext {
    pub fn new(
        mysql_pool: MySqlPool,
        mss_info_config: Arc<MssInfoConfig>,
        telecom_config: Arc<TelecomConfig>,
        clickhouse_config: Arc<ClickhouseConfig>,
    ) -> Result<Arc<Self>> {
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

        Ok(Arc::new(Self {
            mysql_pool,
            http_client,
            mss_info_config,
            gateway_client,
            clickhouse_client,
        }))
    }
}
