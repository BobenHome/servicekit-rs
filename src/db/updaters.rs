use anyhow::Result;
use sqlx::{Execute, MySql, MySqlPool, QueryBuilder};
use tracing::{error, info};

/// 更新 MySQL 表的 `trainNotifyMss` 字段和可选的 `trainNotifyMssMessage` 字段。
///
/// 根据传入的 `table_name` 和 `id_column` 来构建更新语句。
/// `items` 参数是 `(ID, Option<Message>)` 的元组列表。
/// `update_message_field` 参数指示是否应更新 `trainNotifyMssMessage` 字段。
pub async fn update_notify_mss_mysql(
    pool: &MySqlPool,
    table_name: &str,
    id_column: &str,
    status: &str,
    items: &[(String, Option<String>)],
    update_message_field: bool,
) -> Result<u64> {
    if items.is_empty() {
        return Ok(0);
    }

    // 构建 UPDATE ... SET trainNotifyMss = CASE <id_column> WHEN <id_value> THEN <status> ... END
    let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(format!(
        "UPDATE {} SET trainNotifyMss = CASE {} ",
        table_name, id_column
    ));

    // 为每个 item 构建 WHEN ... THEN ... 部分
    for (id, _) in items {
        query_builder.push(" WHEN "); // 推送 SQL 关键字
        query_builder.push_bind(id.clone()); // 绑定 ID 值，sqlx 会为其生成一个 ?
        query_builder.push(" THEN "); // 推送 SQL 关键字
        query_builder.push_bind(status); // 绑定状态值，sqlx 会为其生成一个 ?
    }
    query_builder.push(" END"); // 结束 CASE 语句

    // 根据 `update_message_field` 参数来决定是否包含 `trainNotifyMssMessage` 的更新逻辑
    if update_message_field {
        query_builder.push(format!(", trainNotifyMssMessage = CASE {} ", id_column));
        for (id, msg_opt) in items {
            query_builder.push(" WHEN "); // 推送 SQL 关键字
            query_builder.push_bind(id.clone()); // 绑定 ID 值
            query_builder.push(" THEN "); // 推送 SQL 关键字

            if status == "2" {
                // 失败状态，绑定消息
                query_builder.push_bind(msg_opt.clone()); // 绑定消息值
            } else {
                // 成功状态，明确设置为 NULL
                query_builder.push("NULL ");
            }
        }
        query_builder.push(" END"); // 结束 CASE 语句
    }

    // 构建 WHERE id IN (...)
    query_builder.push(format!(" WHERE {} IN (", id_column));
    let mut separated = query_builder.separated(", ");
    for (id, _) in items {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");

    let query = query_builder.build();

    info!(
        "Executing MySQL update query for table '{}', ID column '{}'. Status: {}, Items count: {}, Update message field: {}",
        table_name, id_column, status, items.len(), update_message_field
    );
    // 打印构建的 SQL 语句和绑定参数，便于调试验证
    info!("Built MySQL update query: {}", query.sql());

    let result = query.execute(pool).await.map_err(|e| {
        error!(
            "Failed to update MySQL table '{}' (status: {}, items: {:?}): {:?}",
            table_name, status, items, e
        );
        e
    })?;

    info!(
        "MySQL update for table '{}' completed. Rows affected: {}",
        table_name,
        result.rows_affected()
    );

    Ok(result.rows_affected())
}
