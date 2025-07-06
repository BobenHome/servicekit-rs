use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use chrono::{Duration, Local};
use reqwest::Client;
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};
use tracing::{error, info};

use crate::utils::mss_client::psn_dos_push;
use crate::{
    ArchivingMssMapper, DynamicPsnData, LecturerData, MssInfoConfig, PushResultParser, TaskExecutor,
};

pub struct PsnLecturerPushTask {
    pub pool: MySqlPool,
    http_client: Client,
    pub mss_info_config: MssInfoConfig,
    archiving_mapper: ArchivingMssMapper,
    push_result_parser: PushResultParser,
}

impl PsnLecturerPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig) -> Self {
        PsnLecturerPushTask {
            http_client: Client::new(),
            mss_info_config: config,
            archiving_mapper: ArchivingMssMapper::new(pool.clone()),
            push_result_parser: PushResultParser::new(pool.clone()),
            pool,
        }
    }

    pub async fn execute_internal(&self) -> Result<()> {
        info!(
            "Running PsnLecturerPushTask task via tokio-cron-scheduler at: {}",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        let mut query_builder =
            QueryBuilder::<MySql>::new(sqlx::query_file!("queries/lecturers.sql").sql());

        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let hit_date = yesterday.format("%Y-%m-%d").to_string();

        query_builder.push(" AND T.hitdate = ");
        query_builder.push_bind(&hit_date);
        query_builder.push(" LIMIT 1 ");

        let lecturer_datas = query_builder
            .build_query_as::<LecturerData>()
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch lecturer data from database")?;

        if lecturer_datas.is_empty() {
            info!("No lecturers found for hitdate: {}", hit_date);
        } else {
            for lecturer_data in lecturer_datas {
                info!("Found lecturer_data: {:?}", lecturer_data);
                // 将 lecturer_data 包装到枚举中
                let psn_data_enum = DynamicPsnData::Lecturer(lecturer_data);
                // 调用通用的 psn_dos_push 函数，传递 self 的引用和数据
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
        info!("PsnLecturerPushTask completed successfully.");

        Ok(())
    }
}

// 实现 TaskExecutor trait
impl TaskExecutor for PsnLecturerPushTask {
    fn execute(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(self.execute_internal()) // 在这里调用实际的异步方法
    }
}
