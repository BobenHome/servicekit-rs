use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use std::str::FromStr;
use std::time::Duration;

pub async fn create_mysql_pool(database_url: &str) -> Result<MySqlPool, sqlx::Error> {
    // 1. MySqlConnectOptions only holds connection details.
    let connect_options = MySqlConnectOptions::from_str(database_url)?;

    // 2. Configure the mysql_pool with timeouts, size, and the after_connect hook.
    let mysql_pool = MySqlPoolOptions::new()
        .max_connections(10)
        .min_connections(2)
        // Set the timeout for creating a new connection HERE.
        .acquire_timeout(Duration::from_secs(3))
        // Finally, build the mysql_pool using the connection details.
        .connect_with(connect_options)
        .await?;

    Ok(mysql_pool)
}
