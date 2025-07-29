use chrono::Local;
use log::{error, info};
use sqlx::MySqlPool;
use uuid::Uuid;

use crate::models::push_result::{MssPushResult, MssPushResultDetail, PushResultService};

pub struct PushResultParser {
    push_result_service: PushResultService, // 持有 PushResultService 实例
}

impl PushResultParser {
    pub fn new(pool: MySqlPool) -> Self {
        PushResultParser {
            push_result_service: PushResultService::new(pool),
        }
    }
    pub async fn parse(&self, data: &str, result: &str) -> Result<(), String> {
        info!("Parsing push result beginning");
        let mut push_result = MssPushResult {
            id: Uuid::new_v4().to_string(),
            push_time: Local::now().naive_local(),
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
                let error_message = format!("Failed to parse result JSON: {:?}", e);
                error!("{}", error_message);
                push_result.error_code = Some("PARSE_ERROR".to_string());
                push_result.error_msg = Some(error_message.clone());
                if let Err(record_err) = self
                    .push_result_service
                    .record(&push_result, &result_details)
                    .await
                {
                    error!("Failed to record result parse error: {:?}", record_err);
                }
                return Err(error_message);
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
                let error_message = format!("Failed to parse request data JSON: {:?}", e);
                error!("{}", error_message);
                push_result.error_msg = Some(error_message.clone());
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
                return Err(error_message);
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
            info!(
                "Parsing push result completed successfully. Result ID: {}",
                push_result.id
            );
            return Ok(());
        }

        // --- 处理失败情况 (code != "200") --
        // 获取 'data' 字段的值，它是一个 JSON 字符串
        let raw_rs_data_str = match result_data.get("data").and_then(|v| v.as_str()) {
            // <--- 尝试将其作为字符串获取
            Some(s) => s,
            None => {
                let error_message = format!("'data' field in result JSON is not a string or does not exist. Cannot extract error details. Full result: {}", result);
                error!("{}", error_message);
                push_result.error_msg = Some(error_message.clone());
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
                return Err(error_message);
            }
        };

        // 对获取到的字符串内容再次进行 JSON 解析
        let rs_data: serde_json::Map<String, serde_json::Value> =
            match serde_json::from_str(raw_rs_data_str) {
                Ok(map) => map,
                Err(e) => {
                    let error_message = format!(
                        "Failed to parse 'data' string as JSON object: {:?}. Original string: {}",
                        e, raw_rs_data_str
                    );
                    error!("{}", error_message);
                    push_result.error_msg = Some(error_message.clone());
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
                    return Err(error_message);
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

        // 推送失败，返回失败
        let final_error_message = push_result
            .error_msg
            .unwrap_or_else(|| format!("Push failed with desc_code: {}", desc_code));
        info!(
            "Parsing push result completed with error. Result ID: {}",
            push_result.id
        );
        Err(final_error_message)
    }
}
