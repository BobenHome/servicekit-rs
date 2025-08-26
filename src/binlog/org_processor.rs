use crate::schedule::binlog_sync::{EntityMetaInfo, ModifyOperationLog};
use crate::AppContext;
use anyhow::{Context, Result};
use chrono::{Local, NaiveDateTime};
// 使用 itertools::Itertools::unique_by 来去重
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{MySql, QueryBuilder, Transaction};
use std::ops::DerefMut;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomOrg {
    pub id: String,
    pub is_delete: Option<bool>,
    pub delete: Option<bool>,
    pub is_corp: Option<bool>,
    pub name: Option<String>,
    pub no: Option<String>,
    pub remark: Option<String>,
    pub abbreviation: Option<String>,
    pub company_info: Option<CompanyInfo>,
    pub contact_info: Option<ContactInfo>,
    pub department_info: Option<DepartmentInfo>,
    pub weight: Option<i32>,
    #[serde(rename = "type")]
    pub type_: Option<i32>,
    pub full_path_id: Option<String>,
    pub full_path_name: Option<String>,
    pub hit_date: Option<String>, // yyyy-MM-dd 格式的日期字符串
    pub in_time: Option<NaiveDateTime>,
    pub year: Option<String>,
    pub month: Option<String>,
    pub hit_date1: Option<NaiveDateTime>,
    #[serde(rename = "entityMetaInfo")]
    pub entity_meta_info: Option<EntityMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanyInfo {
    pub district: Option<String>,
    pub company_nature: Option<String>,
    pub company_type: Option<String>,
    pub company_id: Option<String>,
    pub org_type: Option<String>,
    pub dept_level: Option<String>,
    pub dept_type: Option<String>,
    pub legal: Option<String>,
    pub taxpayer_number: Option<String>,
    pub website: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactInfo {
    pub zip_code: Option<String>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepartmentInfo {
    pub dept_seq: String,
    pub org_type: Option<String>,
    pub dept_level: Option<String>,
    pub dept_type: Option<String>,
    pub leader: Option<String>,
    pub dept_function: Option<String>,
    pub is_cancel: Option<bool>,
    pub is_close: Option<bool>,
    pub found_date: Option<String>,
    pub cancel_date: Option<String>,
    pub close_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomOrgTree {
    pub parent: Option<String>,
    pub level: Option<u8>, // 层级
    pub name: Option<String>,
    pub weight: Option<i32>,
    pub is_corp: Option<bool>,
    pub id: String,
    pub leaf: Option<bool>,
    pub ancestors: Option<Vec<String>>,
    pub full_path_id: Option<String>,
    pub full_path_name: Option<String>,
    pub delete: Option<bool>,
    pub is_delete: Option<bool>,
    #[serde(rename = "entityMetaInfo")]
    pub entity_meta_info: Option<EntityMetaInfo>,
}

impl TelecomOrgTree {
    pub fn get_ancestors(&self) -> String {
        self.ancestors
            .as_ref()
            .map(|ancestors| ancestors.join(","))
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomMssOrgMapping {
    pub code: Option<String>,
    #[serde(rename = "mssCode")]
    pub mss_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelecomMssOrg {
    pub code: Option<String>,
    #[serde(rename = "companyType")]
    pub company_type: Option<String>,
    #[serde(rename = "hrCode")]
    pub hr_code: Option<String>,
    pub sort: Option<f32>,
    #[serde(rename = "type")]
    pub org_type: Option<String>, // 组织类型 例子:Z01
    #[serde(rename = "parentCompanyCode")]
    pub parent_company_code: Option<String>,
    pub name: Option<String>,
    #[serde(rename = "parentDepartmentCode")]
    pub parent_department_code: Option<String>,
    pub id: Option<String>,
    pub status: Option<u8>,
    pub identity: Option<String>,
    #[serde(rename = "departmentType")]
    pub department_type: Option<String>,
    pub time: Option<i64>,
    #[serde(rename = "hitDate")]
    pub hit_date: Option<String>,
    pub year: Option<String>,
    pub month: Option<String>,
    #[serde(rename = "hitDate1")]
    pub hit_date1: Option<NaiveDateTime>,
}

// 用于在处理过程中聚合所有相关数据的结构体
#[derive(Default)]
pub struct ProcessedOrgData {
    pub telecom_orgs: Vec<TelecomOrg>,
    pub telecom_org_trees: Vec<TelecomOrgTree>,
    pub telecom_mss_org_mappings: Vec<TelecomMssOrgMapping>,
    pub telecom_mss_orgs: Vec<TelecomMssOrg>,

    pub org_ids_to_delete: Vec<String>,
    pub org_tree_ids_to_delete: Vec<String>,
    pub org_mapping_codes_to_delete: Vec<String>,
    pub mss_org_codes_to_delete: Vec<String>,
}

// 最大重试次数
const MAX_RETRIES: u32 = 3;

pub struct OrgDataProcessor {
    app_context: Arc<AppContext>,
}

impl OrgDataProcessor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
    /// 主入口函数，包含了重试逻辑
    pub async fn process_orgs_with_retry(&self, mut logs: Vec<ModifyOperationLog>) -> Result<()> {
        for i in 0..MAX_RETRIES {
            if logs.is_empty() {
                info!("所有组织数据都已成功处理。");
                return Ok(());
            }
            info!(
                "开始处理组织数据，剩余 {} 次重试机会。待处理数量: {}",
                MAX_RETRIES - i,
                logs.len()
            );
            let (processed_data, failed_logs) = self.process_batch(logs).await;
            logs = failed_logs; // 更新失败列表，用于下一次重试

            // 如果有成功处理的数据，就保存它们
            if !processed_data.telecom_orgs.is_empty() {
                match self.save_processed_data(processed_data).await {
                    Ok(_) => info!("一批组织数据已成功存入数据库。"),
                    Err(e) => {
                        error!("保存处理后的组织数据失败: {e:?}");
                    }
                }
            }
        }
        // 重试次数用尽后，如果仍有失败的日志，则记录错误
        if !logs.is_empty() {
            error!(
                "组织数据处理重试次数已用尽，仍有 {} 条日志处理失败",
                logs.len()
            );
        }

        Ok(())
    }

    /// 处理一批数据
    /// 返回 (成功处理的数据, 失败的日志)
    async fn process_batch(
        &self,
        logs: Vec<ModifyOperationLog>,
    ) -> (ProcessedOrgData, Vec<ModifyOperationLog>) {
        let mut processed_data = ProcessedOrgData::default();
        let mut failed_logs = Vec::new();

        let now = Local::now().naive_local();
        let year = now.format("%Y").to_string();
        let month = now.format("%m").to_string();

        for log in logs {
            // type 1:add, 2:update
            let need_insert = log.type_ == 1 || log.type_ == 2;

            // 这里是数据转换的核心，我们将它包装在一个 try block 中，方便错误处理
            let result: Result<()> = (async {
                // 1. 转换 TelecomOrg
                if let Some(mut telecom_org) = self
                    .transform_to_telecom_org(&log)
                    .await
                    .context("转换 TelecomOrg error")?
                {
                    processed_data
                        .org_ids_to_delete
                        .push(telecom_org.id.clone());
                    if need_insert {
                        telecom_org.year = Some(year.clone());
                        telecom_org.month = Some(month.clone());
                        telecom_org.in_time = Some(now);
                        telecom_org.hit_date1 = Some(now);
                        telecom_org.hit_date = Some(now.format("%Y-%m-%d").to_string());
                        processed_data.telecom_orgs.push(telecom_org);
                    }
                } else {
                    anyhow::bail!("无法找到对应的 TelecomOrg, log: {log:?}");
                }

                // 2. 转换 OrgTree
                if let Some(org_tree) = self
                    .transform_to_org_tree(&log)
                    .await
                    .context("转换 OrgTree 失败")?
                {
                    processed_data
                        .org_tree_ids_to_delete
                        .push(org_tree.id.clone());
                    if need_insert {
                        processed_data.telecom_org_trees.push(org_tree);
                    }
                } else {
                    anyhow::bail!("无法生成 OrgTree, log: {log:?}");
                }

                // 3. 转换 TelecomMssOrgMapping 和 MssOrganization
                let (mss_org_mapping, mss_code) = self
                    .transform_to_mss_org_mapping(&log)
                    .await
                    .context("转换 TelecomMssOrgMapping 失败")?;

                if let Some(code) = &mss_org_mapping.code {
                    processed_data
                        .org_mapping_codes_to_delete
                        .push(code.clone());
                }
                processed_data
                    .mss_org_codes_to_delete
                    .push(mss_code.clone());

                if need_insert {
                    processed_data
                        .telecom_mss_org_mappings
                        .push(mss_org_mapping);
                }

                let mss_orgs = self
                    .transform_to_mss_orgs(&mss_code)
                    .await
                    .context("查询 TelecomMssOrg 失败")?
                    .ok_or_else(|| anyhow::anyhow!("无法找到 TelecomMssOrg, log: {:?}", log))?;

                if need_insert {
                    for mut mss_org in mss_orgs {
                        mss_org.year = Some(year.clone());
                        mss_org.month = Some(month.clone());
                        mss_org.hit_date1 = Some(now);
                        mss_org.hit_date = Some(now.format("%Y-%m-%d %H:%M:%S").to_string());

                        processed_data.telecom_mss_orgs.push(mss_org);
                    }
                }

                Ok(())
            })
            .await;

            if let Err(e) = result {
                warn!("处理日志失败: {e:?}");
                failed_logs.push(log);
            }
        }
        (processed_data, failed_logs)
    }

    /// 保存处理好的数据到数据库
    async fn save_processed_data(&self, data: ProcessedOrgData) -> Result<()> {
        let mut tx = self.app_context.mysql_pool.begin().await?;
        // --- 1. 执行批量刪除 ---
        info!("开始批量删除旧数据...");
        self.batch_delete(&mut tx, "d_telecom_org", "id", &data.org_ids_to_delete)
            .await?;
        self.batch_delete(
            &mut tx,
            "d_telecom_org_tree",
            "id",
            &data.org_tree_ids_to_delete,
        )
        .await?;
        self.batch_delete(
            &mut tx,
            "d_mss_org_mapping",
            "code",
            &data.org_mapping_codes_to_delete,
        )
        .await?;
        self.batch_delete(
            &mut tx,
            "d_mss_org",
            "hrcode",
            &data.mss_org_codes_to_delete,
        )
        .await?;
        // --- 2. 执行批量插入 ---
        info!("开始批量插入新数据...");
        // 1. 插入 TelecomOrg
        let orgs_to_insert = data
            .telecom_orgs
            .into_iter()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !orgs_to_insert.is_empty() {
            self.batch_insert_telecom_orgs(&mut tx, orgs_to_insert)
                .await?;
        }
        // 2. 插入 TelecomOrgTree
        let org_trees_to_insert = data
            .telecom_org_trees
            .into_iter()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !org_trees_to_insert.is_empty() {
            self.batch_insert_telecom_org_trees(&mut tx, org_trees_to_insert)
                .await?;
        }

        // 3. 插入 TelecomMssOrgMapping
        let mss_org_mappings_to_insert = data
            .telecom_mss_org_mappings
            .into_iter()
            .unique_by(|o| o.code.clone())
            .collect::<Vec<_>>();
        if !mss_org_mappings_to_insert.is_empty() {
            self.batch_insert_telecom_mss_org_mappings(&mut tx, mss_org_mappings_to_insert)
                .await?;
        }

        // 4. 插入 TelecomMssOrg
        let mss_orgs_to_insert = data
            .telecom_mss_orgs
            .into_iter()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !mss_orgs_to_insert.is_empty() {
            self.batch_insert_telecom_mss_orgs(&mut tx, mss_orgs_to_insert)
                .await?
        }
        tx.commit().await?;
        Ok(())
    }

    async fn transform_to_telecom_org(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<Option<TelecomOrg>> {
        let cid = log
            .cid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("CID is missing for log {}", log.id))?;

        self.app_context.gateway_client.org_loadbyid(cid).await
    }

    async fn transform_to_org_tree(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<Option<TelecomOrgTree>> {
        let cid = log
            .cid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("CID is missing for log {}", log.id))?;

        self.app_context.gateway_client.org_tree_loadbyid(cid).await
    }

    async fn transform_to_mss_org_mapping(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<(TelecomMssOrgMapping, String)> {
        let cid = log
            .cid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("CID is missing for log {}", log.id))?;

        let mapping = self
            .app_context
            .gateway_client
            .mss_organization_translate(cid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("MSS organization not found for CID: {}", cid))?;

        let mss_code = mapping
            .mss_code
            .clone()
            .ok_or_else(|| anyhow::anyhow!("MSS code is missing for mapping"))?;

        Ok((mapping, mss_code))
    }

    async fn transform_to_mss_orgs(&self, mss_code: &str) -> Result<Option<Vec<TelecomMssOrg>>> {
        self.app_context
            .gateway_client
            .mss_organization_query(mss_code)
            .await
    }

    async fn batch_insert_telecom_orgs(
        &self,
        tx: &mut Transaction<'_, MySql>,
        orgs: Vec<TelecomOrg>,
    ) -> Result<()> {
        if orgs.is_empty() {
            return Ok(());
        }
        // 使用 QueryBuilder 安全地构建批量插入语句
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_telecom_org (
            no,
            datelastmodified,
            department_info_is_close,
            department_info_is_cancel,
            name,
            company_type,
            company_id,
            org_type,
            weight,
            is_corp,
            id,
            contact_info,
            remark,
            abbreviation,
            dept_level,
            dept_type,
            legal,
            taxpayer_number,
            website,
            d_delete,
            is_delete,
            hitdate,
            intime,
            year,
            month,
            hitdate1,
            amount
        ) ",
        );
        query_builder.push_values(orgs, |mut b, org| {
            // 转换 Option<bool> 为 Option<String>
            let is_corp_str = org.is_corp.map(|b| b.to_string());
            let is_delete_str = org.is_delete.map(|b| b.to_string());
            let delete_str = org.delete.map(|b| b.to_string());

            let department_info_is_close = org
                .department_info
                .as_ref()
                .and_then(|d| d.is_close.map(|b| b.to_string()));
            let department_info_is_cancel = org
                .department_info
                .as_ref()
                .and_then(|d| d.is_cancel.map(|b| b.to_string()));

            b.push_bind(org.no)
                .push_bind(
                    org.entity_meta_info
                        .map(|e| e.date_last_modified)
                        .unwrap_or_default(),
                )
                .push_bind(department_info_is_close)
                .push_bind(department_info_is_cancel)
                .push_bind(org.name)
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.company_type.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.company_id.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.org_type.clone())
                        .unwrap_or_default(),
                )
                .push_bind(org.weight)
                .push_bind(is_corp_str)
                .push_bind(org.id)
                .push_bind(None::<String>) // contact_info 设为 NULL
                .push_bind(org.remark)
                .push_bind(org.abbreviation)
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.dept_level.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.dept_type.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.legal.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.taxpayer_number.clone())
                        .unwrap_or_default(),
                )
                .push_bind(
                    org.company_info
                        .as_ref()
                        .map(|c| c.website.clone())
                        .unwrap_or_default(),
                )
                .push_bind(delete_str)
                .push_bind(is_delete_str)
                .push_bind(org.hit_date)
                .push_bind(org.in_time)
                .push_bind(org.year)
                .push_bind(org.month)
                .push_bind(org.hit_date1)
                .push_bind(None::<String>); // amount 设为 NULL
        });
        let query = query_builder.build();
        query.execute(tx.deref_mut()).await?;
        Ok(())
    }

    async fn batch_insert_telecom_org_trees(
        &self,
        tx: &mut Transaction<'_, MySql>,
        org_trees: Vec<TelecomOrgTree>,
    ) -> Result<()> {
        if org_trees.is_empty() {
            return Ok(());
        }
        // 使用 QueryBuilder 安全地构建批量插入语句
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_telecom_org_tree (
            parent,
            ENTITYMETAINFO_DATECREATED,
            DATELASTMODIFIED,
            D_LEVEL,
            NAME,
            WEIGHT,
            IS_CORP,
            ID,
            LEAF,
            ANCESTORS,
            D_DELETE,
            IS_DELETE
        ) ",
        );
        query_builder.push_values(org_trees, |mut b, org_tree| {
            // 提前提取所有需要的值，避免所有权问题
            let date_created = org_tree
                .entity_meta_info
                .as_ref()
                .and_then(|e| e.date_created);
            let date_last_modified = org_tree
                .entity_meta_info
                .as_ref()
                .and_then(|e| e.date_last_modified);
            let ancestors = org_tree.get_ancestors();
            let is_corp_str = org_tree.is_corp.map(|b| b.to_string());

            b.push_bind(org_tree.parent)
                .push_bind(date_created)
                .push_bind(date_last_modified)
                .push_bind(org_tree.level)
                .push_bind(org_tree.name)
                .push_bind(org_tree.weight)
                .push_bind(is_corp_str)
                .push_bind(org_tree.id)
                .push_bind(org_tree.leaf)
                .push_bind(ancestors)
                .push_bind(org_tree.delete)
                .push_bind(org_tree.is_delete);
        });
        let query = query_builder.build();
        query.execute(&mut **tx).await?;
        Ok(())
    }

    async fn batch_insert_telecom_mss_org_mappings(
        &self,
        tx: &mut Transaction<'_, MySql>,
        mss_org_mappings: Vec<TelecomMssOrgMapping>,
    ) -> Result<()> {
        if mss_org_mappings.is_empty() {
            return Ok(());
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_mss_org_mapping (
            code,
            msscode
        ) ",
        );
        query_builder.push_values(mss_org_mappings, |mut b, mss_org_mapping| {
            b.push_bind(mss_org_mapping.code)
                .push_bind(mss_org_mapping.mss_code);
        });
        let query = query_builder.build();
        query.execute(&mut **tx).await?;
        Ok(())
    }

    async fn batch_insert_telecom_mss_orgs(
        &self,
        tx: &mut Transaction<'_, MySql>,
        mss_orgs: Vec<TelecomMssOrg>,
    ) -> Result<()> {
        if mss_orgs.is_empty() {
            return Ok(());
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO d_mss_org (
            code,
            companytype,
            hrcode,
            sort,
            type,
            parentcompanycode,
            identity,
            name,
            parentdepartmentcode,
            id,
            time,
            status,
            hitdate1,
            hitdate,
            year,
            month,
            amount
        ) ",
        );
        query_builder.push_values(mss_orgs, |mut b, mss_org| {
            b.push_bind(mss_org.code)
                .push_bind(mss_org.company_type)
                .push_bind(mss_org.hr_code)
                .push_bind(mss_org.sort)
                .push_bind(mss_org.org_type)
                .push_bind(mss_org.parent_company_code)
                .push_bind(mss_org.identity)
                .push_bind(mss_org.name)
                .push_bind(mss_org.parent_department_code)
                .push_bind(mss_org.id)
                .push_bind(mss_org.time)
                .push_bind(mss_org.status)
                .push_bind(mss_org.hit_date1)
                .push_bind(mss_org.hit_date)
                .push_bind(mss_org.year)
                .push_bind(mss_org.month)
                .push_bind(None::<String>); // amount 设为 NULL
        });
        let query = query_builder.build();
        query.execute(&mut **tx).await?;
        Ok(())
    }

    async fn batch_delete(
        &self,
        tx: &mut Transaction<'_, MySql>,
        table_name: &str,
        key_name: &str,
        ids: &[String],
    ) -> Result<()> {
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
            "在表 {} 中刪除了 {} 条记录",
            table_name,
            result.rows_affected()
        );
        Ok(())
    }
}
