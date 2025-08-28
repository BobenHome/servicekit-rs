use crate::schedule::binlog_sync::{EntityMetaInfo, ModifyOperationLog};
use crate::utils::MapToProcessError;
use crate::utils::ProcessError;
use crate::AppContext;
use anyhow::Result;
use chrono::{Local, NaiveDateTime};
use itertools::Itertools; // 使用 itertools::Itertools::unique_by 来去重
use serde::{Deserialize, Serialize};
use sqlx::{MySql, QueryBuilder, Transaction};
use std::ops::DerefMut;
use std::sync::Arc;
use tracing::{error, info};

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

impl ProcessedOrgData {
    /// 将另一个 ProcessedOrgData 合并到自身
    pub fn merge(&mut self, other: &mut ProcessedOrgData) {
        self.telecom_orgs.append(&mut other.telecom_orgs);
        self.telecom_org_trees.append(&mut other.telecom_org_trees);
        self.telecom_mss_org_mappings
            .append(&mut other.telecom_mss_org_mappings);
        self.telecom_mss_orgs.append(&mut other.telecom_mss_orgs);

        self.org_ids_to_delete.append(&mut other.org_ids_to_delete);
        self.org_tree_ids_to_delete
            .append(&mut other.org_tree_ids_to_delete);
        self.org_mapping_codes_to_delete
            .append(&mut other.org_mapping_codes_to_delete);
        self.mss_org_codes_to_delete
            .append(&mut other.mss_org_codes_to_delete);
    }
}

// 最大重试次数
const MAX_RETRIES: u32 = 3;

// 2. 定义处理状态机，用于保存每个日志的处理进度
#[derive(Debug)]
enum ProcessingState {
    // 初始状态，只有原始日志
    Initial(ModifyOperationLog),
    // 成功获取 TelecomOrg，保存下来
    GotTelecomOrg(ModifyOperationLog, TelecomOrg),
    // 成功获取 OrgTree，保存之前的结果
    GotOrgTree(ModifyOperationLog, TelecomOrg, TelecomOrgTree),
    // 成功获取 MssMapping，保存之前的所有结果
    GotMssMapping(
        ModifyOperationLog,
        TelecomOrg,
        TelecomOrgTree,
        TelecomMssOrgMapping,
        String,
    ),
}

// 用于记录永久失败的日志和原因
struct PermanentFailure {
    log: ModifyOperationLog,
    reason: String,
}

// 表示状态转换的结果
enum Transition {
    // 状态成功向前推进，这是新的状态
    Advanced(ProcessingState),
    // 所有步骤已成功完成，并携带最后一步获取的数据
    Completed(
        ModifyOperationLog,
        TelecomOrg,
        TelecomOrgTree,
        TelecomMssOrgMapping,
        String,
        Vec<TelecomMssOrg>,
    ),
}

pub struct OrgDataProcessor {
    app_context: Arc<AppContext>,
}

impl OrgDataProcessor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
    /// 主入口函数，包含了重试逻辑
    pub async fn process_orgs_with_retry(&self, logs: Vec<ModifyOperationLog>) -> Result<()> {
        // 将原始日志初始化为状态机的初始状态
        let mut states_to_process: Vec<ProcessingState> =
            logs.into_iter().map(ProcessingState::Initial).collect();

        let mut final_processed_data = ProcessedOrgData::default();

        for i in 0..MAX_RETRIES {
            if states_to_process.is_empty() {
                info!("所有组织数据都已成功处理。");
                break;
            }
            info!(
                "开始处理组织数据，剩余 {} 次重试机会。待处理数量: {}",
                MAX_RETRIES - i,
                states_to_process.len()
            );

            let (mut processed_data_chunk, next_states, permanent_failures) =
                self.advance_states(states_to_process).await;

            // 合并当轮成功的数据
            final_processed_data.merge(&mut processed_data_chunk);

            // 记录永久失败的日志
            if !permanent_failures.is_empty() {
                for failure in permanent_failures {
                    error!(
                        "日志处理永久失败，将不再重试。原因: {}. Log: {:?}",
                        failure.reason, failure.log
                    );
                }
            }
            // 更新待处理列表，用于下一轮重试
            states_to_process = next_states;
        }
        // 重试次数用尽后，如果仍有未处理的状态，则记录错误
        if !states_to_process.is_empty() {
            error!(
                "组织数据处理重试次数已用尽，仍有 {} 条日志未处理完成。",
                states_to_process.len()
            );
        }
        // 所有轮次结束后，一次性保存所有成功的数据
        match self.save_processed_data(final_processed_data).await {
            Ok(_) => info!("所有批次的组织数据已成功存入数据库。"),
            Err(e) => error!("最终保存处理后的组织数据失败: {e:?}"),
        }

        Ok(())
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
    ) -> Result<Option<TelecomOrg>, ProcessError> {
        let cid = log
            .cid
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("CID is missing for log {}", log.id))?;

        self.app_context
            .gateway_client
            .org_loadbyid(cid)
            .await
            .map_gateway_err()
    }

    async fn transform_to_org_tree(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<Option<TelecomOrgTree>, ProcessError> {
        let cid = log.cid.as_deref().ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("CID is missing for log {}", log.id))
        })?;

        self.app_context
            .gateway_client
            .org_tree_loadbyid(cid)
            .await
            .map_gateway_err()
    }

    async fn transform_to_mss_org_mapping(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<(TelecomMssOrgMapping, String), ProcessError> {
        // 1. 处理逻辑错误：如果 CID 缺失，这是一个永久性错误
        let cid = log.cid.as_deref().ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("CID is missing for log {}", log.id))
        })?;

        // 2. 处理网络调用：在这里区分超时错误和其它错误
        let mapping_option = self
            .app_context
            .gateway_client
            .mss_organization_translate(cid)
            .await
            .map_gateway_err()?;

        // 3. 处理逻辑错误：如果 API 调用成功但没有返回数据，这是一个永久性错误
        let mapping = mapping_option.ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!(
                "MSS organization not found for CID: {}",
                cid
            ))
        })?;

        // 4. 处理逻辑错误：如果返回的数据缺少必要的 mss_code 字段，这也是一个永久性错误
        let mss_code = mapping.mss_code.clone().ok_or_else(|| {
            ProcessError::Permanent(anyhow::anyhow!("MSS code is missing for mapping"))
        })?;

        Ok((mapping, mss_code))
    }

    async fn transform_to_mss_orgs(
        &self,
        mss_code: &str,
    ) -> Result<Option<Vec<TelecomMssOrg>>, ProcessError> {
        self.app_context
            .gateway_client
            .mss_organization_query(mss_code)
            .await
            .map_gateway_err()
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

    /// 状态机处理函数，驱动每个日志的状态向前演进
    async fn advance_states(
        &self,
        states: Vec<ProcessingState>,
    ) -> (
        ProcessedOrgData,      // 本轮成功完成处理的数据
        Vec<ProcessingState>,  // 需要重试的状态
        Vec<PermanentFailure>, // 永久失败的日志
    ) {
        let mut processed_data = ProcessedOrgData::default();
        let mut states_for_retry = Vec::new();
        let mut permanent_failures = Vec::new();

        let now = Local::now().naive_local();
        let year = now.format("%Y").to_string();
        let month = now.format("%m").to_string();

        for state in states {
            let mut current_state = state;
            // 使用 loop 来驱动单个日志的状态流转，直到成功、需要重试或永久失败
            loop {
                // 注意：这里传递的是引用，避免不必要的 clone
                let next_transition_result = match &current_state {
                    ProcessingState::Initial(log) => self.handle_initial_state(log.clone()).await,
                    ProcessingState::GotTelecomOrg(log, org) => {
                        self.handle_got_telecom_org_state(log.clone(), org.clone())
                            .await
                    }
                    ProcessingState::GotOrgTree(log, org, tree) => {
                        self.handle_got_org_tree_state(log.clone(), org.clone(), tree.clone())
                            .await
                    }
                    ProcessingState::GotMssMapping(log, org, tree, mapping, mss_code) => {
                        self.handle_got_mss_mapping_state(
                            log.clone(),
                            org.clone(),
                            tree.clone(),
                            mapping.clone(),
                            mss_code.clone(),
                        )
                        .await
                    }
                };

                match next_transition_result {
                    // 状态成功推进
                    Ok(Transition::Advanced(next_state)) => {
                        // 核心逻辑：立即处理上一个状态的数据
                        match &next_state {
                            ProcessingState::GotTelecomOrg(log, org) => {
                                // 从 Initial -> GotTelecomOrg，处理 org
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                processed_data.org_ids_to_delete.push(org.id.clone());
                                if need_insert {
                                    let mut org_to_insert = org.clone();
                                    org_to_insert.year = Some(year.clone());
                                    org_to_insert.month = Some(month.clone());
                                    org_to_insert.in_time = Some(now);
                                    org_to_insert.hit_date1 = Some(now);
                                    org_to_insert.hit_date =
                                        Some(now.format("%Y-%m-%d").to_string());
                                    processed_data.telecom_orgs.push(org_to_insert);
                                }
                            }
                            ProcessingState::GotOrgTree(log, _, tree) => {
                                // 从 GotTelecomOrg -> GotOrgTree，处理 tree
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                processed_data.org_tree_ids_to_delete.push(tree.id.clone());
                                if need_insert {
                                    processed_data.telecom_org_trees.push(tree.clone());
                                }
                            }
                            ProcessingState::GotMssMapping(log, _, _, mapping, mss_code) => {
                                // 从 GotOrgTree -> GotMssMapping，处理 mapping 和 mss_code
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                if let Some(code) = &mapping.code {
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
                                        .push(mapping.clone());
                                }
                            }
                            _ => {}
                        }
                        // 更新状态，继续循环
                        current_state = next_state;
                    }
                    // 所有步骤都已成功完成
                    Ok(Transition::Completed(log, _, _, _, _, mss_orgs)) => {
                        // 处理最后一步 mss_orgs 的数据
                        let need_insert = log.type_ == 1 || log.type_ == 2;
                        if need_insert {
                            for mut mss_org in mss_orgs {
                                mss_org.year = Some(year.clone());
                                mss_org.month = Some(month.clone());
                                mss_org.hit_date1 = Some(now);
                                mss_org.hit_date =
                                    Some(now.format("%Y-%m-%d %H:%M:%S").to_string());
                                processed_data.telecom_mss_orgs.push(mss_org);
                            }
                        }
                        break; // 此日志处理完成，跳出 loop
                    }
                    // 发生超时，将当前状态加入重试列表
                    Err(ProcessError::GatewayTimeout(_)) => {
                        states_for_retry.push(current_state);
                        break; // 跳出 loop，处理下一条日志
                    }
                    // 发生永久性错误，记录并放弃
                    Err(ProcessError::Permanent(e)) => {
                        let log = match current_state {
                            ProcessingState::Initial(log) => log,
                            ProcessingState::GotTelecomOrg(log, ..) => log,
                            ProcessingState::GotOrgTree(log, ..) => log,
                            ProcessingState::GotMssMapping(log, ..) => log,
                        };
                        permanent_failures.push(PermanentFailure {
                            log,
                            reason: e.to_string(),
                        });
                        break; // 跳出 loop，处理下一条日志
                    }
                }
            }
        }
        info!("states_for_retry {:?} len: {}", states_for_retry, states_for_retry.len());
        (processed_data, states_for_retry, permanent_failures)
    }

    // --- 为每个状态创建一个独立的辅助处理函数，使逻辑更清晰 ---
    async fn handle_initial_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        match self.transform_to_telecom_org(&log).await? {
            // 成功获取，返回 Advanced 状态
            Some(org) => Ok(Transition::Advanced(ProcessingState::GotTelecomOrg(
                log, org,
            ))),
            None => Err(ProcessError::Permanent(anyhow::anyhow!(
                "无法找到对应的 TelecomOrg"
            ))),
        }
    }

    async fn handle_got_telecom_org_state(
        &self,
        log: ModifyOperationLog,
        org: TelecomOrg,
    ) -> Result<Transition, ProcessError> {
        match self.transform_to_org_tree(&log).await? {
            Some(tree) => Ok(Transition::Advanced(ProcessingState::GotOrgTree(
                log, org, tree,
            ))),
            None => Err(ProcessError::Permanent(anyhow::anyhow!("无法生成 OrgTree"))),
        }
    }

    async fn handle_got_org_tree_state(
        &self,
        log: ModifyOperationLog,
        org: TelecomOrg,
        tree: TelecomOrgTree,
    ) -> Result<Transition, ProcessError> {
        let (mapping, mss_code) = self.transform_to_mss_org_mapping(&log).await?;
        // 成功获取，返回 Advanced 状态
        Ok(Transition::Advanced(ProcessingState::GotMssMapping(
            log, org, tree, mapping, mss_code,
        )))
    }

    async fn handle_got_mss_mapping_state(
        &self,
        log: ModifyOperationLog,
        org: TelecomOrg,
        tree: TelecomOrgTree,
        mapping: TelecomMssOrgMapping,
        mss_code: String,
    ) -> Result<Transition, ProcessError> {
        let mss_orgs = self
            .transform_to_mss_orgs(&mss_code)
            .await?
            .ok_or_else(|| ProcessError::Permanent(anyhow::anyhow!("无法找到 TelecomMssOrg")))?;

        // 这是最后一步，成功后返回 Completed 状态，并携带所有数据
        Ok(Transition::Completed(
            log, org, tree, mapping, mss_code, mss_orgs,
        ))
    }
}
