use std::future::Future;
use std::pin::Pin;

use crate::mappers::archiving_mss_mapper::ArchivingMssMapper;
use crate::{ClassData, DynamicPsnData, TaskExecutor};
use anyhow::{Context, Result}; // 导入 anyhow::Result 和 Context trait
use chrono::{Duration, Local};
// 导入 log 宏
use log::{error, info};
use reqwest::Client;
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};
// 导入 PushResultParser
use crate::parsers::push_result_parser::PushResultParser;
// 导入 MssInfoConfig
use crate::config::MssInfoConfig;
use crate::utils::mss_client::psn_dos_push;

pub struct PsnTrainPushTask {
    pub pool: MySqlPool,
    http_client: Client,
    pub mss_info_config: MssInfoConfig,
    archiving_mapper: ArchivingMssMapper,
    push_result_parser: PushResultParser,
    pub task_name: String, // 添加任务名称字段
}

impl PsnTrainPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig) -> Self {
        PsnTrainPushTask {
            // pool 的所有权在这里被 ArchivingMssMapper 拿走了一部分，
            // 但 MySqlPool 是可克隆的，所以可以为 mapper 克隆一份
            http_client: Client::new(),
            mss_info_config: config,
            archiving_mapper: ArchivingMssMapper::new(pool.clone()), // 克隆 pool 给 mapper
            push_result_parser: PushResultParser::new(pool.clone()),
            pool, // PsntrainPushTask 自身也需要持有 pool，用于 execute 方法中的查询
            task_name: "PsnTrainPushTask".to_string(), // 设置默认任务名称
        }
    }

    pub async fn execute_internal(&self) -> Result<()> {
        info!(
            "Running {} via tokio-cron-scheduler at: {}",
            self.name(),
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let hit_date = yesterday.format("%Y-%m-%d").to_string();

        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(&hit_date);
        query_builder.push(" LIMIT 1 ");

        let class_datas = query_builder
            .build_query_as::<ClassData>()
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch trains from database")?; // 将数据库错误转换为 anyhow::Error 并添加上下文

        if class_datas.is_empty() {
            info!("No trains found for hitdate: {}", hit_date);
        } else {
            for class_data in class_datas {
                info!("Found class_data: {:?}", class_data);
                // 将 ClassData 包装到枚举中
                let psn_data_enum = DynamicPsnData::Class(class_data);
                if let Err(e) = psn_dos_push(
                    &self.http_client,
                    &self.mss_info_config,
                    &self.archiving_mapper,
                    &self.push_result_parser,
                    &psn_data_enum,
                )
                .await
                {
                    error!(
                        "Failed to send data of type '{}' to third party: {:?}",
                        psn_data_enum.get_key_name(),
                        e
                    );
                } else {
                    info!(
                        "Successfully sent data of type '{}' to third party.",
                        psn_data_enum.get_key_name()
                    );
                }
            }
        }
        info!("{} completed successfully.", self.name());
        Ok(()) // 如果一切正常，返回 Ok(())
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnTrainPushTask {
    fn name(&self) -> &str {
        &self.task_name // 返回任务名称
    }

    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(self.execute_internal()) // 在这里调用实际的异步方法
    }
}
