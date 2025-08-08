use anyhow::{Context, Result};
use log::info;
use serde::Serialize;
use sqlx::MySqlPool;

#[derive(Debug, Clone, Serialize)] // Serialize for eventual logging/db storage if needed
pub struct RecordMssReply {
    pub id: String,
    pub datas: String,
    pub send_time: String,
    pub msg: String,
}

// 模拟数据库 mapper
pub struct ArchivingMssMapper {
    mysql_pool: MySqlPool, // ArchivingMssMapper 现在持有数据库连接池
}

impl ArchivingMssMapper {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        ArchivingMssMapper { mysql_pool }
    }

    pub async fn record_mss_reply(&self, reply: &RecordMssReply) -> Result<()> {
        info!("Recording MSS reply to DB, ID: {:?}", reply.id);
        // 使用 sqlx::query! 或 sqlx::query_as! 进行插入
        // 这里是关键：明确指定数据库列名
        sqlx::query!(
            r#"
            INSERT INTO data_archiving_mss_record (id, msg, datas, sendTime)
            VALUES (?, ?, ?, ?)
            "#,
            reply.id,
            reply.msg,
            reply.datas,
            reply.send_time
        )
        .execute(&self.mysql_pool)
        .await
        .context("Failed to insert RecordMssReply into data_archiving_mss_record")?;

        Ok(())
    }
}
