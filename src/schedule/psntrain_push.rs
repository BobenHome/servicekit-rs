use crate::models::push_result::{MssPushResult, MssPushResultDetail, PushResultService};
use crate::{ClassData, DynamicPsnData};
use anyhow::{anyhow, Context, Result}; // 导入 anyhow::Result 和 Context trait
use chrono::{Duration, Local};
// 导入 log 宏
use log::{error, info, warn};
use reqwest::{Client, StatusCode};
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

pub struct PsnTrainPushTask {
    pub pool: MySqlPool,
    http_client: Client,
    mss_info_config: MssInfoConfig,
    archiving_mapper: ArchivingMssMapper,
    push_result_parser: PushResultParser,
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
    async fn psn_dos_push(&self, psn_data: &DynamicPsnData) -> Result<()> {
        let mut request_attempt = 0;
        const MAX_RETRIES: u32 = 5;

        let dynamic_key_name = psn_data.get_key_name();

        let request_json_data_value = json!({
            dynamic_key_name: [psn_data]
        });

        let request_json_data = serde_json::to_string(&request_json_data_value)
            .context("Failed to serialize dynamic JSON payload")?;

        let mut final_http_body_str: String = String::new(); // 最终的响应体

        // 引入一个 Result 来封装循环体内的逻辑，以便统一错误处理
        let result_of_send_loop: Result<(), anyhow::Error> = loop {
            request_attempt += 1;
            info!(
                "Attempting to send data to {} (Attempt {}), key: {}",
                self.mss_info_config.app_url, request_attempt, dynamic_key_name
            );

            if request_attempt > 1 {
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
                .body(request_json_data.clone());

            let response_result = request.send().await;

            let current_http_body_str: String;
            let current_status: StatusCode;

            match response_result {
                Ok(response) => {
                    current_status = response.status();
                    match response.text().await {
                        Ok(body) => current_http_body_str = body,
                        Err(e) => {
                            // 读取响应体失败
                            error!(
                                "Failed to read response body for {}: {:?}",
                                self.mss_info_config.app_url, e
                            );
                            break Err(anyhow!(
                                "Failed to read response body for {}: {:?}",
                                self.mss_info_config.app_url,
                                e
                            ));
                        }
                    }
                }
                Err(e) => {
                    // 发送请求失败 (网络不通, DNS 查找失败等)
                    error!(
                        "Failed to send HTTP request to {}: {:?}",
                        self.mss_info_config.app_url, e
                    );
                    break Err(anyhow!(
                        "Failed to send HTTP request to {}: {:?}",
                        self.mss_info_config.app_url,
                        e
                    ));
                }
            }

            info!(
                "Received response for {} (Attempt {}): Status={}, Body={}",
                self.mss_info_config.app_url,
                request_attempt,
                current_status,
                current_http_body_str
            );

            if current_status.is_success() {
                if have_rest(&current_http_body_str) {
                    if request_attempt >= MAX_RETRIES {
                        error!(
                            "Max retries reached. Still have 'rest' condition. Body: {}",
                            current_http_body_str
                        );
                        break Err(anyhow!(
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
                    final_http_body_str = current_http_body_str; // 成功时赋值
                    break Ok(()); // 成功并退出重试循环
                }
            } else {
                // HTTP 状态码表示失败
                error!(
                    "HTTP request to {} failed with status: {}. Body: {}",
                    self.mss_info_config.app_url, current_status, current_http_body_str
                );
                break Err(anyhow!(
                    "HTTP request failed with status: {}. Body: {}",
                    current_status,
                    current_http_body_str
                ));
            }
        };

        // 统一的错误处理和记录逻辑
        let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let primary_result = match result_of_send_loop {
            Ok(_) => {
                // 请求成功，记录成功信息
                let record_reply = RecordMssReply {
                    id: Uuid::new_v4().to_string().replace("-", ""),
                    datas: format!(
                        "X-APP_ID{}|X-APP-KEY{}|DATA:{}",
                        self.mss_info_config.app_id,
                        self.mss_info_config.app_key,
                        request_json_data
                    ),
                    send_time: current_time,
                    msg: final_http_body_str.clone(), // 成功时使用 final_http_body_str
                };
                // 尝试记录成功信息，如果记录失败，将记录的错误链到主结果上
                self.archiving_mapper
                    .record_mss_reply(&record_reply)
                    .await
                    .context("Failed to record SUCCESS MSS reply")?; // 使用 ? 传播数据库写入错误

                // 只有成功时才调用 parser.parse
                self.push_result_parser
                    .parse(&request_json_data, &final_http_body_str)
                    .await;
                Ok(()) // 主请求和记录都成功
            }
            Err(e) => {
                // 请求失败，记录失败信息
                let error_message = format!("ERROR: {:?}", e); // 捕获并格式化错误
                error!(
                    "Attempted to send data to third party failed: {}",
                    error_message
                );

                let record_reply_error = RecordMssReply {
                    id: Uuid::new_v4().to_string().replace("-", ""),
                    datas: format!("sendDATA:{}", request_json_data), // 记录发送的数据
                    send_time: current_time,
                    msg: error_message, // 记录错误消息
                };
                // 尝试记录失败信息，如果记录失败，将记录的错误链到主结果上
                self.archiving_mapper
                    .record_mss_reply(&record_reply_error)
                    .await
                    .context("Failed to record FAILED MSS reply")?; // 使用 ? 传播数据库写入错误
                                                                    // 返回原始的失败结果，以便 execute 方法能知道发生了错误
                Err(e)
            }
        };
        primary_result // 返回主结果，它包含了 send_loop 的结果以及记录的结果
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
pub struct PushResultParser {
    push_result_service: PushResultService, // 持有 PushResultService 实例
}

impl PushResultParser {
    pub fn new(pool: MySqlPool) -> Self {
        PushResultParser {
            push_result_service: PushResultService::new(pool),
        }
    }
    pub async fn parse(&self, data: &str, result: &str) {
        info!("Parsing push result: data='{}', result='{}'", data, result);
        let mut push_result = MssPushResult {
            id: Uuid::new_v4().to_string().replace("-", ""),
            push_time: Local::now(),
            train_id: None,
            course_id: None,
            user_id: None,
            data_type: None,
            error_msg: None,
            error_code: None,
        };
        let mut result_details: Vec<MssPushResultDetail> = Vec::new();

        // 1. 解析 'result' JSON
        let result_data: serde_json::Value = match serde_json::from_str(result) {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to parse result JSON: {:?}", e);
                push_result.error_code = Some("PARSE_ERROR".to_string());
                push_result.error_msg = Some(format!("Failed to parse result JSON: {:?}", e));
                if let Err(record_err) = self
                    .push_result_service
                    .record(&push_result, &result_details)
                    .await
                {
                    error!("Failed to record result parse error: {:?}", record_err);
                }
                return;
            }
        };

        let desc_code = result_data
            .get("descCode")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN_CODE")
            .to_string();
        push_result.error_code = Some(desc_code.clone());

        // 2. 解析 'data' JSON 并提取核心 ID
        let request_data: serde_json::Value = match serde_json::from_str(data) {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to parse request data JSON: {:?}", e);
                push_result.error_msg = Some(format!("Failed to parse request data JSON: {:?}", e));
                if let Err(record_err) = self
                    .push_result_service
                    .record(&push_result, &result_details)
                    .await
                {
                    error!(
                        "Failed to record request data parse error: {:?}",
                        record_err
                    );
                }
                return;
            }
        };

        // 辅助函数，用于从请求数据中提取相关字段并设置 push_result
        let keys_to_check_request_data = vec![
            ("classData", 1, "trainingId", "train_id"),
            ("lecturerData", 2, "course_id", "course_id"),
            ("psnTrainingData", 3, "userId", "user_id"),
            ("psnArchiveData", 4, "userId", "user_id"),
        ];

        for (key, data_type_val, id_field, push_result_field) in keys_to_check_request_data {
            if let Some(json_array) = request_data.get(key).and_then(|v| v.as_array()) {
                if let Some(first_obj) = json_array.get(0).and_then(|v| v.as_object()) {
                    if let Some(id_value) = first_obj.get(id_field).and_then(|v| v.as_str()) {
                        push_result.data_type = Some(data_type_val);
                        let detail_id_string = id_value.to_string();

                        match push_result_field {
                            "train_id" => push_result.train_id = Some(detail_id_string.clone()),
                            "course_id" => push_result.course_id = Some(detail_id_string.clone()),
                            "user_id" => push_result.user_id = Some(detail_id_string.clone()),
                            _ => {}
                        }

                        let detail = MssPushResultDetail {
                            data_id: push_result.id.clone(),
                            result_id: Some(detail_id_string),
                        };
                        result_details.push(detail);
                        // Java 逻辑会遍历所有，所以这里不 `break`
                    }
                }
            }
        }

        // 3. 根据 desc_code 进行条件记录
        if desc_code == "200" {
            if let Err(record_err) = self
                .push_result_service
                .record(&push_result, &result_details)
                .await
            {
                error!("Failed to record successful push result: {:?}", record_err);
            }
            return; // 成功后返回，与 Java 行为一致
        }

        // --- 处理失败情况 (code != "200") --
        // 获取 'data' 字段的值，它是一个 JSON 字符串
        let raw_rs_data_str = match result_data.get("data").and_then(|v| v.as_str()) {
            // <--- 尝试将其作为字符串获取
            Some(s) => s,
            None => {
                error!("'data' field in result JSON is not a string or does not exist. Cannot extract error details.");
                push_result.error_msg = Some(format!(
                    "Failed to parse error details: 'data' field is not a string or missing. Full result: {}",
                    result
                ));
                if let Err(record_err) = self
                    .push_result_service
                    .record(&push_result, &result_details)
                    .await
                {
                    error!(
                        "Failed to record push result due to error parsing failure details: {:?}",
                        record_err
                    );
                }
                return;
            }
        };

        // 对获取到的字符串内容再次进行 JSON 解析
        let rs_data: serde_json::Map<String, serde_json::Value> =
            match serde_json::from_str(raw_rs_data_str) {
                Ok(map) => map,
                Err(e) => {
                    error!(
                        "Failed to parse 'data' string as JSON object: {:?}. Original string: {}",
                        e, raw_rs_data_str
                    );
                    push_result.error_msg = Some(format!(
                        "Failed to parse 'data' string as JSON object: {:?}. Original string: {}",
                        e, raw_rs_data_str
                    ));
                    if let Err(record_err) = self
                        .push_result_service
                        .record(&push_result, &result_details)
                        .await
                    {
                        error!(
                            "Failed to record push result due to nested JSON parsing failure: {:?}",
                            record_err
                        );
                    }
                    return;
                }
            };

        // 现在 rs_data 是一个真正的 JSON 对象 Map，可以直接使用了
        // 原始的 if let 语句被替换为直接使用 rs_data
        let error_keys_to_check = vec![
            ("classData", 1, "trainingId"),
            ("lecturerData", 2, "course_id"),
            ("psnTrainingData", 3, "userId"),
            ("psnArchiveData", 4, "userId"),
        ];

        // 清空之前从 request_data 填充的详情和字段，因为现在要根据响应结果填充
        result_details.clear();
        push_result.train_id = None;
        push_result.course_id = None;
        push_result.user_id = None;
        push_result.data_type = None; // 也重置类型

        for (key, data_type_val, id_field_name) in error_keys_to_check {
            // 注意这里现在直接从 rs_data 中获取，而不是 rs_data.get(key).and_then(|v| v.as_array())
            // 因为 rs_data 本身已经是 Map
            if let Some(json_array) = rs_data.get(key).and_then(|v| v.as_array()) {
                if let Some(first_obj) = json_array.get(0).and_then(|v| v.as_object()) {
                    push_result.data_type = Some(data_type_val); // 更新类型
                                                                 // 从错误响应中提取所有相关字段
                    if let Some(train_id) = first_obj.get("trainingId").and_then(|v| v.as_str()) {
                        push_result.train_id = Some(train_id.to_string());
                    }
                    if let Some(course_id) = first_obj.get("course_id").and_then(|v| v.as_str()) {
                        push_result.course_id = Some(course_id.to_string());
                    }
                    if let Some(user_id) = first_obj.get("userId").and_then(|v| v.as_str()) {
                        push_result.user_id = Some(user_id.to_string());
                    }
                    if let Some(error_msg) = first_obj.get("errormsg").and_then(|v| v.as_str()) {
                        push_result.error_msg = Some(error_msg.to_string());
                    }
                    if let Some(error_code) = first_obj.get("errorcode").and_then(|v| v.as_str()) {
                        push_result.error_code = Some(error_code.to_string());
                    }

                    // 基于错误响应的 ID 重新添加 PushResultDetail
                    let mut current_detail_id: Option<String> = None;
                    if let Some(id_val) = first_obj.get(id_field_name).and_then(|v| v.as_str()) {
                        current_detail_id = Some(id_val.to_string());
                    }

                    let detail = MssPushResultDetail {
                        data_id: push_result.id.clone(),
                        result_id: current_detail_id,
                    };
                    result_details.push(detail);
                }
            }
        }
        // 最终记录失败情况
        if let Err(record_err) = self
            .push_result_service
            .record(&push_result, &result_details)
            .await
        {
            error!("Failed to record failed push result: {:?}", record_err);
        }
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
