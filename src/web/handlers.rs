use crate::{
    web::models::{ApiResponse, QueryParams},
    PsnClass,
};
use actix_web::{get, web, HttpResponse, Result};
use sqlx::Execute;
use sqlx::{MySql, MySqlPool, QueryBuilder};
use serde_json::{json, Value};
use sqlx::Row;

#[get("/trains")]
pub async fn get_trains(
    pool: web::Data<MySqlPool>,
    query: web::Query<QueryParams>,
) -> Result<HttpResponse> {
    let hit_date = query.hit_date.as_deref();
    let train_ids = query
        .train_ids
        .as_deref()
        .map(|s| s.split(',').collect::<Vec<_>>());

    // 使用 QueryBuilder 构建查询
    // ...existing query building logic...
    let mut query_builder =
        QueryBuilder::<MySql>::new(sqlx::query_file!("queries/trains.sql").sql());

    if let Some(date) = hit_date {
        query_builder.push(" AND a.hitdate = ");
        query_builder.push_bind(date);
    }

    if let Some(ids) = train_ids {
        if !ids.is_empty() {
            query_builder.push(" AND a.TRAINID IN (");
            let mut separated = query_builder.separated(", ");
            for id in ids {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");
        }
    }

    match query_builder
        .build_query_as::<PsnClass>()
        .fetch_all(pool.get_ref())
        .await
    {
        Ok(trains) => Ok(HttpResponse::Ok().json(ApiResponse::<Vec<PsnClass>>::success(trains))),
        Err(e) => Ok(HttpResponse::InternalServerError()
            .json(ApiResponse::<Vec<PsnClass>>::error(e.to_string()))),
    }
}

#[get("/trains/{id}")]
pub async fn get_train_by_id(
    pool: web::Data<MySqlPool>,
    id: web::Path<String>,
) -> Result<HttpResponse> {
    match sqlx::query(
        r#"SELECT 
            a.ID AS _id,
            a.ID AS id,
            a.DATASTATE AS operation,
            a.TRAINID AS training_id,
            a.TRAINNAME AS training_name,
            tl.`NAME` AS train_level,
            tm.`NAME` AS train_mode,
            tc.`NAME` AS train_category
        FROM NU_TRAINSOURCEDATA_xzs_hyk a
            LEFT JOIN MC_ORG_SHOW b ON a.supDeptCode = b.id
            LEFT JOIN MC_ORG_SHOW o ON a.ORGANIZERID = o.id
            LEFT JOIN fz_train_trainlevel tl on tl.`CODE` = a.TRAINLEVEL
            LEFT JOIN fz_train_trainmode tm on tm.`CODE` = a.TRAINMODE
            LEFT JOIN fz_train_traincategory tc on tc.`CODE` = a.TRAINCATEGORY
        WHERE a.TRAINID = ?"#
    )
    .bind(id.as_ref())
    .fetch_optional(pool.get_ref())
    .await
    {
        Ok(Some(row)) => {
            let json_response = json!({
                "_id": row.get::<Option<String>, _>("_id").unwrap_or_default(),
                "id": row.get::<Option<String>, _>("id").unwrap_or_default(),
                "operation": row.get::<Option<String>, _>("operation").unwrap_or_default(),
                "training_id": row.get::<Option<String>, _>("training_id").unwrap_or_default(),
                "training_name": row.get::<Option<String>, _>("training_name").unwrap_or_default(),
                "train_level": row.get::<Option<String>, _>("train_level"),
                "train_mode": row.get::<Option<String>, _>("train_mode"),
                "train_category": row.get::<Option<String>, _>("train_category")
            });
            Ok(HttpResponse::Ok().json(ApiResponse::<Value>::success(json_response)))
        },
        Ok(None) => Ok(HttpResponse::NotFound().json(ApiResponse::<Value>::error("Train not found".into()))),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<Value>::error(e.to_string())))
    }
}
