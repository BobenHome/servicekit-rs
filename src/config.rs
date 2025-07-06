use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub web_server_port: u16,
    pub tasks: TasksConfig, // <--- 新增：包含所有任务的配置
    pub mss_info_config: MssInfoConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TasksConfig {
    pub psn_push: PsnPushTaskConfig, // <--- 示例：针对 PsnPushTaskConfig 的配置
                                     // pub another_task: AnotherTaskConfig, // <--- 如果有其他任务，可以在这里添加
}

#[derive(Debug, Deserialize, Clone)]
pub struct PsnPushTaskConfig {
    pub cron_schedule: String, // <--- 将 cron_schedule 移到任务自己的配置中
}

#[derive(Debug, Deserialize, Clone)]
pub struct MssInfoConfig {
    pub app_id: String,
    pub app_key: String,
    pub app_url: String,
}

impl AppConfig {
    pub fn new() -> Result<Self, config::ConfigError> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name("config/default")) // 从 config/default.toml 加载
            .add_source(config::Environment::with_prefix("APP").separator("__")); // 允许环境变量覆盖 (例如: APP__TASKS__PSN_TRAIN_PUSH__CRON_SCHEDULE)

        builder.build()?.try_deserialize()
    }
}
