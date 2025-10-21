use std::sync::Arc;

use crate::binlog::processor::DataProcessorTrait;
use crate::binlog::{OrgDataProcessor, UserDataProcessor};
use crate::schedule::binlog_sync::{DataType, ModifyOperationLog};
use crate::web::BinlogParams;
use crate::{web::models::ApiResponse, AppContext};
use actix_web::{post, web, HttpResponse, Result};
use tracing::{error, info, warn};

#[post("/binlog/sync")]
pub async fn binlog_sync(
    app_context: web::Data<Arc<AppContext>>, // 注入 AppContext
    body: web::Json<BinlogParams>,           // 接收 JSON 请求体
) -> Result<HttpResponse> {
    // 克隆必要的配置和连接池，以便在异步任务中使用
    let app_context = Arc::clone(&app_context);
    // 1. 获取 BinlogParams 的所有权
    let params = body.into_inner();
    tokio::spawn(async move {
        info!("----------------binlog org sync begin----------------");
        // 2. 构造 logs
        let logs: Vec<ModifyOperationLog> = params
            .ids
            .into_iter()
            .map(|id| ModifyOperationLog {
                id: uuid::Uuid::new_v4().to_string(),
                cid: Some(id),
                type_: 1,
                ..Default::default()
            })
            .collect();

        let data_type = params.data_type;
        match data_type {
            DataType::Org => {
                let org_processor = OrgDataProcessor::new(Arc::clone(&app_context));
                // 返回Result，让上层决定如何处理错误
                if let Err(e) = org_processor.process(logs).await {
                    error!("Error occurred while manual processing organization data: {e:?}");
                } else {
                    info!("Organization data manual processing completed.");
                }
            }
            DataType::User => {
                let user_processor = UserDataProcessor::new(Arc::clone(&app_context));
                if let Err(e) = user_processor.process(logs).await {
                    error!("Error occurred while manual processing user data: {e:?}");
                } else {
                    info!("User data manual processing completed.");
                }
            }
            _ => {
                warn!("Unknown or unsupported DataType for processing: {data_type:?}");
            }
        };
        info!("----------------binlog org sync end----------------");
    });

    // 立即返回成功响应，因为处理是异步的
    Ok(HttpResponse::Ok().json(ApiResponse::<String>::success(
        "syncing, check logs for progress.".to_string(),
    )))
}
