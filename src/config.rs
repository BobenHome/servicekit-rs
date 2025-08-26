use std::sync::Arc;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub web_server_port: u16,
    pub tasks: TasksConfig, // 包含所有任务的配置
    #[serde(skip)] // 序列化/反序列化时跳过，因为我们会在 new 方法中手动处理 Arc 包装
    pub mss_info_config: Arc<MssInfoConfig>,
    #[serde(skip)]
    pub telecom_config: Arc<TelecomConfig>, // 电信相关配置
    #[serde(skip)]
    pub clickhouse_config: Arc<ClickhouseConfig>, // ClickHouse配置
    #[serde(skip)]
    pub redis_config: Arc<RedisConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TasksConfig {
    pub psn_push: PsnPushTaskConfig,
    pub binlog_sync: BinlogSyncTaskConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PsnPushTaskConfig {
    pub cron_schedule: String,
    pub task_name: String, // 任务名称
}

#[derive(Debug, Deserialize, Clone)]
pub struct BinlogSyncTaskConfig {
    pub cron_schedule: String,
    pub task_name: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MssInfoConfig {
    pub app_id: String,
    pub app_key: String,
    pub app_url: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Targets {
    pub newtca: u32,
    pub basedata: u32,
    pub mss: u32,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TelecomConfig {
    pub gateway_url: String,
    pub source_app_id: u32,
    pub mode: i32,
    pub is_sync: bool,
    pub targets: Targets,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ClickhouseConfig {
    pub hosts: Vec<String>,
    pub ports: Vec<u16>,
    pub user: String,
    pub password: String,
    pub database: String,
}

// 添加一个临时的结构体用于初始反序列化
#[derive(Debug, Deserialize)]
struct RawAppConfig {
    pub database_url: String,
    pub web_server_port: u16,
    pub tasks: TasksConfig,
    pub mss_info_config: MssInfoConfig,
    pub telecom_config: TelecomConfig,
    pub clickhouse_config: ClickhouseConfig,
    pub redis_config: RedisConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RedisConfig {
    pub url: String,
}

impl AppConfig {
    pub fn new() -> Result<Self, config::ConfigError> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name("config/default.toml"))
            .add_source(config::Environment::with_prefix("APP").separator("__")); // 允许环境变量覆盖 (例如: APP__TASKS__PSN_TRAIN_PUSH__CRON_SCHEDULE)

        // 使用 try_deserialize 来直接反序列化为 RawAppConfig
        // 在反序列化后手动将相关字段包装到 Arc 中，并返回 AppConfig
        let raw_config: RawAppConfig = builder.build()?.try_deserialize()?;
        Ok(AppConfig {
            database_url: raw_config.database_url,
            web_server_port: raw_config.web_server_port,
            tasks: raw_config.tasks,
            mss_info_config: Arc::new(raw_config.mss_info_config),
            telecom_config: Arc::new(raw_config.telecom_config),
            clickhouse_config: Arc::new(raw_config.clickhouse_config),
            redis_config: Arc::new(raw_config.redis_config),
        })
    }
}
