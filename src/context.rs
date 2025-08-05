use std::sync::Arc;

use crate::config::{MssInfoConfig, TelecomConfig};
use crate::utils::{ClickHouseClient, GatewayClient};
use crate::ClickhouseConfig;
use anyhow::{Context as _, Result};
use sqlx::MySqlPool;
use tracing::info;

#[derive(Clone)]
pub struct AppContext {
    pub pool: MySqlPool,
    pub mss_info_config: Arc<MssInfoConfig>,
    pub gateway_client: Arc<GatewayClient>,
    pub clickhouse_client: Arc<ClickHouseClient>,
}

impl AppContext {
    pub fn new(
        pool: MySqlPool,
        mss_info_config: Arc<MssInfoConfig>,
        telecom_config: Arc<TelecomConfig>,
        clickhouse_config: Arc<ClickhouseConfig>,
    ) -> Result<Arc<Self>> {
        // --- Initialize GatewayClient ---
        let gateway_client = Arc::new(GatewayClient::new(telecom_config));
        info!("GatewayClient initialized.");
        // --- Initialize ClickHouseClient ---
        let clickhouse_client = Arc::new(
            ClickHouseClient::new(clickhouse_config)
                .context("Failed to initialize ClickHouseClient")?,
        );
        info!("ClickHouseClient initialized.");

        Ok(Arc::new(Self {
            pool,
            mss_info_config,
            gateway_client,
            clickhouse_client,
        }))
    }
}
