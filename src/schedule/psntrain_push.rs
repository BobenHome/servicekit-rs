use crate::{ClassData, DynamicPsnData};
use anyhow::{anyhow, Context, Result}; // 导入 anyhow::Result 和 Context trait
use chrono::{Duration, Local};
// 导入 log 宏
use log::{error, info, warn};
use reqwest::Client;
use serde::Serialize;
use serde_json::{from_str, json, Value};
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};
use uuid::Uuid;

// 模拟数据库 mapper
pub struct ArchivingMssMapper {
    pool: MySqlPool, // ArchivingMssMapper 现在持有数据库连接池
}

impl ArchivingMssMapper {
    pub fn new(pool: MySqlPool) -> Self {
        ArchivingMssMapper { pool }
    }

    pub async fn record_mss_reply(&self, reply: &RecordMssReply) -> Result<()> {
        info!("Recording MSS reply to DB: {:?}", reply);
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
        .execute(&self.pool)
        .await
        .context("Failed to insert RecordMssReply into data_archiving_mss_record")?;

        Ok(())
    }
}

pub struct PsntrainPushTask {
    pub pool: MySqlPool,
    http_client: Client,
    mss_info_config: MssInfoConfig,
    archiving_mapper: ArchivingMssMapper,
    push_result_parser: PushResultParser,
}

impl PsntrainPushTask {
    pub fn new(pool: MySqlPool, config: MssInfoConfig) -> Self {
        PsntrainPushTask {
            // pool 的所有权在这里被 ArchivingMssMapper 拿走了一部分，
            // 但 MySqlPool 是可克隆的，所以可以为 mapper 克隆一份
            http_client: Client::new(),
            mss_info_config: config,
            archiving_mapper: ArchivingMssMapper::new(pool.clone()), // 克隆 pool 给 mapper
            push_result_parser: PushResultParser::new(),
            pool, // PsntrainPushTask 自身也需要持有 pool，用于 execute 方法中的查询
        }
    }

    // 这只是一个普通的公共异步方法，不再是 trait 的一部分
    pub async fn execute(&self) -> Result<()> {
        info!(
            "Running task via tokio-cron-scheduler at: {}",
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
                // 调用内部的私有方法
                if let Err(e) = self.psn_dos_push(&psn_data_enum).await {
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
        info!("PsntrainPushTask completed successfully.");
        Ok(()) // 如果一切正常，返回 Ok(())
    }

    // 我们保留这个方法，以便在 main.rs 中方便地获取 Cron 表达式
    pub fn cron_expression(&self) -> &str {
        // tokio-cron-scheduler 完美支持这种格式。
        "0 */1 * * * *" // 每5分钟执行一次
    }

    // 将 send_psn_to_third_party 变为 PsntrainPushTask 的私有方法
    // 这样它就可以直接访问 self 的字段，而不需要将 mapper, client, config 等作为参数传递
    async fn psn_dos_push(
        &self,                     // 注意这里是 &self
        psn_data: &DynamicPsnData, // 接受枚举类型
    ) -> Result<()> {
        // 记录请求尝试次数
        let mut request_attempt = 0;
        // 限制最大重试次数，防止无限循环
        const MAX_RETRIES: u32 = 5;
        // 动态获取键名
        let dynamic_key_name = psn_data.get_key_name();
        // 动态构建 JSON 请求体
        let request_json_data_value = json!({
            dynamic_key_name: [psn_data]// 直接序列化枚举变体
        });

        let request_json_data = serde_json::to_string(&request_json_data_value)
            .context("Failed to serialize dynamic JSON payload")?;

        // 在这里声明，并在循环外部保留最终结果
        let final_http_body_str: String; // <--- 声明为只在循环结束后赋值
        loop {
            request_attempt += 1;
            info!(
                "Attempting to send data to {} (Attempt {}), key: {}",
                self.mss_info_config.app_url, request_attempt, dynamic_key_name
            );

            if request_attempt > 1 {
                // 第一次发送前没有 Thread.sleep(20)，所以这里只在重试时睡眠
                info!("********Resting 1 minute before retry********");
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }

            let request = self
                .http_client
                .post(&self.mss_info_config.app_url)
                .header("X-APP-ID", &self.mss_info_config.app_id)
                .header("X-APP-KEY", &self.mss_info_config.app_key)
                .header("Content-Type", "application/json")
                .body(request_json_data.clone()); // 克隆请求体，以便重试时再次发送

            let response = request.send().await.context(format!(
                "Failed to send HTTP request to {}",
                self.mss_info_config.app_url
            ))?;

            let status = response.status();
            // 直接在这里声明并赋值，它只在当前循环迭代中有效
            let current_http_body_str = response
                .text()
                .await
                .context("Failed to read response body")?;

            info!(
                "Received response for {} (Attempt {}): Status={}, Body={}",
                self.mss_info_config.app_url, request_attempt, status, current_http_body_str
            );

            if status.is_success() {
                if have_rest(&current_http_body_str) {
                    if request_attempt >= MAX_RETRIES {
                        error!(
                            "Max retries reached. Still have 'rest' condition. Body: {}",
                            current_http_body_str
                        );
                        return Err(anyhow!(
                            "Max retries reached for {}. Still requires rest.",
                            self.mss_info_config.app_url
                        ));
                    }
                    info!("Response indicates 'rest' required. Retrying...");
                    continue; // 继续循环进行重试
                } else {
                    info!(
                        "Request to {} successful and no 'rest' required.",
                        self.mss_info_config.app_url
                    );
                    final_http_body_str = current_http_body_str; // <--- 赋值给最终变量
                    break; // 成功并退出重试循环
                }
            } else {
                error!(
                    "HTTP request to {} failed with status: {}. Body: {}",
                    self.mss_info_config.app_url, status, current_http_body_str
                );
                return Err(anyhow!(
                    "HTTP request failed with status: {}. Body: {}",
                    status,
                    current_http_body_str
                ));
            }
        }
        // 循环结束后，这里才真正使用 final_http_body_str
        let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let record_reply = RecordMssReply {
            id: Uuid::new_v4().to_string().replace("-", ""),
            datas: format!(
                "X-APP_ID{}|X-APP-KEY{}|DATA:{}",
                self.mss_info_config.app_id, self.mss_info_config.app_key, request_json_data
            ),
            send_time: current_time,
            msg: final_http_body_str.clone(),
        };

        // 直接通过 self 访问 archiving_mapper
        self.archiving_mapper
            .record_mss_reply(&record_reply)
            .await
            .context("Failed to record MSS reply")?;

        // 直接通过 self 访问 push_result_parser
        self.push_result_parser
            .parse(&request_json_data, &final_http_body_str);

        Ok(())
    }
}

// 假设 RecordMssReply 和 ArchivingMssMapper
// 您需要根据实际的数据库操作库（如 sqlx）来定义它们
// 这是一个模拟的 RecordMssReply
#[derive(Debug, Clone, Serialize)] // Serialize for eventual logging/db storage if needed
pub struct RecordMssReply {
    pub id: String,
    pub datas: String,
    pub send_time: String,
    pub msg: String,
}

// 模拟 PushResultParser
pub struct PushResultParser;

impl PushResultParser {
    pub fn new() -> Self {
        PushResultParser {}
    }
    pub fn parse(&self, data: &str, http_body: &str) {
        info!(
            "Parsing push result: data='{}', http_body='{}'",
            data, http_body
        );
        // 这里是你的解析逻辑
    }
}

// Configuration for the third-party service
pub struct MssInfoConfig {
    pub app_url: String,
    pub app_id: String,
    pub app_key: String,
}

impl MssInfoConfig {
    pub fn new(app_url: String, app_id: String, app_key: String) -> Self {
        MssInfoConfig {
            app_url,
            app_id,
            app_key,
        }
    }
}

/// 检查 HTTP 响应体是否指示需要“休息”（重试）
fn have_rest(http_body: &str) -> bool {
    // 1. 检查 httpBody 是否为空 JSON 对象字符串
    if "{}" == http_body.trim() {
        // 使用 .trim() 处理可能的空白字符
        return false;
    }
    // 2. 尝试将 httpBody 解析为 JSON
    let json_value: Value = match from_str(http_body) {
        Ok(val) => val,
        Err(e) => {
            // 如果解析失败，说明不是有效的 JSON，则不认为需要“休息”
            warn!(
                "Failed to parse http_body as JSON in have_rest: {:?}. Body: '{}'",
                e, http_body
            );
            return false;
        }
    };

    // 3. 判断是否是 dcoos 返回的那种 json，并检查 "code" 字段
    if let Some(obj) = json_value.as_object() {
        // 确保是 JSON 对象
        if let Some(code_value) = obj.get("code") {
            // 获取 "code" 字段的值
            if let Some(code_str) = code_value.as_str() {
                // 确保 "code" 是字符串
                // 4. 检查 "code" 字段的值是否为 "9019"
                if code_str == "9019" {
                    return true;
                }
            }
        }
    }
    // 默认情况或不满足条件时返回 false
    false
}
