use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MssPushResult {
    pub id: String, // 数据库中存储为 VARCHAR(36)
    pub push_time: NaiveDateTime,
    pub train_id: Option<String>,
    pub course_id: Option<String>,
    pub user_id: Option<String>,
    pub data_type: Option<i32>, // `type` 是 SQL 关键字，我们使用 `data_type`
    pub error_msg: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MssPushResultDetail {
    pub data_id: String,           // 关联到 MssPushResult.id
    pub result_id: Option<String>, // 可以是 trainingId, course_id, userId 等
}

pub struct PushResultService {
    mysql_pool: MySqlPool,
}

impl PushResultService {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        PushResultService { mysql_pool }
    }

    pub async fn record(
        &self,
        mss_push_result: &MssPushResult,
        result_details: &[MssPushResultDetail],
    ) -> Result<()> {
        // 插入 MssPushResult 主记录
        sqlx::query!(
            r#"
            INSERT INTO mss_push_result (id, push_time, train_id, course_id, user_id, type, error_msg, error_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            mss_push_result.id,
            mss_push_result.push_time,
            mss_push_result.train_id,
            mss_push_result.course_id,
            mss_push_result.user_id,
            mss_push_result.data_type,
            mss_push_result.error_msg,
            mss_push_result.error_code,
        )
        .execute(&self.mysql_pool)
        .await
        .context("Failed to insert into mss_push_result table")?;

        // 插入 MssPushResultDetail 详情记录
        for detail in result_details {
            sqlx::query!(
                r#"
                INSERT INTO mss_push_result_detail (data_id, result_id)
                VALUES (?, ?)
                "#,
                detail.data_id,
                detail.result_id,
            )
            .execute(&self.mysql_pool)
            .await
            .context("Failed to insert into mss_push_result_detail table")?;
        }

        Ok(())
    }
}
