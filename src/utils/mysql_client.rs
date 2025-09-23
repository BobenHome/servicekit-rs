use itertools::Itertools;
use sqlx::{MySql, Transaction};
use std::ops::DerefMut;
use tracing::info;

pub async fn batch_delete(
    tx: &mut Transaction<'_, MySql>,
    table_name: &str,
    key_name: &str,
    ids: &[String],
) -> anyhow::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    // 对 ID 进行去重
    let unique_ids: Vec<_> = ids.iter().unique().collect();
    // 构建 `DELETE FROM table WHERE id IN (?, ?, ...)` 查詢
    let query_str = format!(
        "DELETE FROM {} WHERE {} IN ({})",
        table_name,
        key_name,
        unique_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",")
    );
    let mut query = sqlx::query(&query_str);
    for id in unique_ids {
        query = query.bind(id);
    }
    let result = query.execute(tx.deref_mut()).await?; // 修改这一行
    info!(
        "Deleted {} records in table {}",
        result.rows_affected(),
        table_name
    );
    Ok(())
}
