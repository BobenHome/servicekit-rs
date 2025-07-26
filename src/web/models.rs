use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub begin_date: Option<String>,
    pub end_date: Option<String>,
    pub train_ids: Option<String>,
}

// 为新的 POST 接口定义请求参数结构体
#[derive(Debug, Deserialize)]
pub struct PushDataParams {
    pub begin_date: Option<String>,     // 日期范围的开始日期
    pub end_date: Option<String>,       // 日期范围的结束日期
    pub train_ids: Option<Vec<String>>, // 培训 ID 列表
}

impl PushDataParams {
    // 验证参数的互斥性
    pub fn validate(&self) -> Result<(), String> {
        let has_dates = self.begin_date.is_some() || self.end_date.is_some();
        let has_ids = self.train_ids.is_some();

        match (has_dates, has_ids) {
            (true, true) => Err(
                "Cannot provide both date range (begin_date/end_date) and train_ids.".to_string(),
            ),
            (false, false) => Err(
                "Must provide either a date range (begin_date/end_date) or train_ids.".to_string(),
            ),
            (true, false) => {
                // 如果提供了日期，确保 begin_date 和 end_date 都存在
                if self.begin_date.is_none() || self.end_date.is_none() {
                    Err(
                        "Both begin_date and end_date must be provided if using date range."
                            .to_string(),
                    )
                } else {
                    Ok(())
                }
            }
            (false, true) => Ok(()), // 只提供了 trainIds，合理
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message),
        }
    }
}
