use crate::schedule::binlog_sync::{EntityMetaInfo, ModifyOperationLog};
use crate::utils::MapToProcessError;
use crate::utils::ProcessError;
use crate::AppContext;
use anyhow::Result;
use chrono::{Local, NaiveDateTime};
use itertools::Itertools; // 使用 itertools::Itertools::unique_by 来去重
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::{Execute, MySql, QueryBuilder, Transaction};
use std::ops::DerefMut;
use std::sync::{Arc, OnceLock};
use tracing::{error, info};

// 定义静态Regex（全局或模块级，确保只编译一次）
static CITY_CLEAN_RE: OnceLock<Regex> = OnceLock::new();

fn get_city_clean_re() -> &'static Regex {
    CITY_CLEAN_RE.get_or_init(|| {
        Regex::new(r"(分公司|电信分公司\*|中国电信股份有限公司|市|分公司\*|中国电信)").unwrap()
    })
}
// 浙江特殊情况，第6个元素还是浙江，要取第7个元素
const SPECIAL_CITY_MARKER: &str = "4843217f-e083-44a4-adc3-c85f25448af8";

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
    pub dept_seq: Option<String>,
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
    #[serde(rename = "chiefLeader")]
    pub chief_leader: Option<String>,
    #[serde(rename = "deputyLeader")]
    pub deputy_leader: Option<String>,
    #[serde(rename = "departmentLevel")]
    pub department_level: Option<String>,
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
    GotTelecomOrg(ModifyOperationLog, Box<TelecomOrg>), // 将大的字段（如 TelecomOrg）包装在 Box 里，让枚举变体本身变得非常小，从而让整个枚举都变得小巧
    // 成功获取 OrgTree，保存之前的结果
    GotOrgTree(ModifyOperationLog, Box<TelecomOrgTree>),
    // 成功获取 MssMapping，保存之前的所有结果
    GotMssMapping(ModifyOperationLog, TelecomMssOrgMapping, String),
}

// 用于记录永久失败的日志和原因
struct PermanentFailure {
    log: ModifyOperationLog,
    reason: String,
}

// 表示状态转换的结果
enum Transition {
    // 状态成功向前推进，这是新的状态
    Advanced(Box<ProcessingState>),
    // 所有步骤已成功完成，并携带最后一步获取的数据
    Completed(Box<ModifyOperationLog>, Vec<TelecomMssOrg>),
}

pub struct OrgDataProcessor {
    app_context: Arc<AppContext>,
}

impl OrgDataProcessor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
    /// 主入口函数，包含了重试逻辑
    pub async fn process_orgs(&self, logs: Vec<ModifyOperationLog>) -> Result<()> {
        // 将原始日志初始化为状态机的初始状态
        let mut states_to_process: Vec<ProcessingState> =
            logs.into_iter().map(ProcessingState::Initial).collect();

        let mut final_processed_data = ProcessedOrgData::default();

        for i in 0..MAX_RETRIES {
            if states_to_process.is_empty() {
                info!("All organization data has been successfully processed.");
                break;
            }
            info!(
                "Processing organization data, {} retry attempts remaining. Pending count: {}",
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
                        "Processing permanently failed, will not retry. Reason: {}. Log: {:?}",
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
                "Maximum retries reached, {} logs still unprocessed.",
                states_to_process.len()
            );
        }
        // 所有轮次结束后，一次性保存所有成功的数据
        match self.save_processed_data(&final_processed_data).await {
            Ok(_) => info!("All batches of organization data successfully saved to database."),
            Err(e) => error!("Failed to refresh mc_org_show table: {e:?}"),
        }

        // 在 d_* 表更新成功后，调用刷新 mc_org_show 的逻辑
        if let Err(e) = self.refresh_mc_org_show(&final_processed_data).await {
            error!("Failed to refresh mc_org_show table: {e:?}");
        }

        Ok(())
    }

    /// 保存处理好的数据到数据库
    async fn save_processed_data(&self, data: &ProcessedOrgData) -> Result<()> {
        let mut tx = self.app_context.mysql_pool.begin().await?;
        // --- 1. 执行批量刪除 ---
        info!("Starting batch deletion of old data...");
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
        info!("Starting batch insertion of new data...");
        // 1. 插入 TelecomOrg
        let orgs_to_insert = data
            .telecom_orgs
            .iter()
            .cloned()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !orgs_to_insert.is_empty() {
            self.batch_insert_telecom_orgs(&mut tx, orgs_to_insert)
                .await?;
        }
        // 2. 插入 TelecomOrgTree
        let org_trees_to_insert = data
            .telecom_org_trees
            .iter()
            .cloned()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !org_trees_to_insert.is_empty() {
            self.batch_insert_telecom_org_trees(&mut tx, org_trees_to_insert)
                .await?;
        }

        // 3. 插入 TelecomMssOrgMapping
        let mss_org_mappings_to_insert = data
            .telecom_mss_org_mappings
            .iter()
            .cloned()
            .unique_by(|o| o.code.clone())
            .collect::<Vec<_>>();
        if !mss_org_mappings_to_insert.is_empty() {
            self.batch_insert_telecom_mss_org_mappings(&mut tx, mss_org_mappings_to_insert)
                .await?;
        }

        // 4. 插入 TelecomMssOrg
        let mss_orgs_to_insert = data
            .telecom_mss_orgs
            .iter()
            .cloned()
            .unique_by(|o| o.id.clone())
            .collect::<Vec<_>>();
        if !mss_orgs_to_insert.is_empty() {
            self.batch_insert_telecom_mss_orgs(&mut tx, mss_orgs_to_insert)
                .await?
        }
        tx.commit().await?;
        Ok(())
    }

    /// 根据受影响的组织ID，增量刷新 mc_org_show 表
    async fn refresh_mc_org_show(&self, data: &ProcessedOrgData) -> Result<()> {
        // 1. 收集本次批次所有受影响的、唯一的组织ID
        let mut affected_ids = data
            .org_ids_to_delete
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for org in &data.telecom_orgs {
            affected_ids.insert(org.id.clone());
        }
        let unique_affected_ids: Vec<String> = affected_ids.into_iter().collect();

        if unique_affected_ids.is_empty() {
            info!("No organization data changes, no need to refresh mc_org_show.");
            return Ok(());
        }
        info!(
            "Starting refresh of mc_org_show table, affected organization ID count: {}",
            unique_affected_ids.len()
        );
        // 2. 开启一个新的事务来处理刷新逻辑
        let mut tx = self.app_context.mysql_pool.begin().await?;

        // 3. (Delete) 先从 mc_org_show 中删除所有受影响的记录
        self.batch_delete(&mut tx, "mc_org_show", "ID", &unique_affected_ids)
            .await?;

        // 4. (Insert) 重新计算并插入需要存在的数据
        //    只为那些需要新增或更新的组织（即存在于 telecom_orgs 列表中的）执行插入
        let ids_to_insert: Vec<String> = data.telecom_orgs.iter().map(|o| o.id.clone()).collect();

        if !ids_to_insert.is_empty() {
            // 4.1. 从 .sql 文件加载原始SQL
            let raw_sql_query = sqlx::query_file!("queries/refresh_mc_org_show.sql");

            // 4.2. 使用 QueryBuilder 附加动态的 WHERE IN 子句
            let mut query_builder = QueryBuilder::new(raw_sql_query.sql());
            query_builder.push(" WHERE TE.ID IN (");
            let mut separated = query_builder.separated(", ");
            for id in &ids_to_insert {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");

            // 4.3. 构建并执行最终的查询
            let final_query = query_builder.build();
            let result = final_query.execute(tx.deref_mut()).await?;

            info!(
                "Inserted {} new records into mc_org_show",
                result.rows_affected()
            );
        }
        // 5. 提交事务
        tx.commit().await?;
        info!("mc_org_show table refresh complete.");

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
            C_CODE,
            PROVINCE,
            P_CODE,
            CITY,
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
            amount,
            full_path_id,
            full_path_name
        ) ",
        );
        query_builder.push_values(orgs, |mut b, org| {
            // 转换 Option<bool> 为 Option<String>
            let is_corp_str = org.is_corp.map(|b| b.to_string());
            let is_delete_str = org.is_delete.map(|b| b.to_string());
            let delete_str = org.delete.map(|b| b.to_string());

            let cleaned_name = org.name.map(|n| n.trim().replace('\u{200b}', ""));

            let mut p_code: Option<String> = None;
            let mut province_name: Option<String> = None;
            let mut c_code: Option<String> = None;
            let mut city_index: usize = 5; // 默认取第6个元素（索引5）

            if let Some(path) = &org.full_path_id {
                let parts: Vec<&str> = path.split(',').collect();
                // 提取 省份ID (P_CODE)，它是路径中的第5个元素 (索引为4)
                if let Some(province_id_str) = parts.get(4) {
                    // 查找省份名称
                    province_name = self.app_context.provinces.get(*province_id_str).cloned();
                    p_code = Some(province_id_str.to_string());
                }

                // 决定用于城市的索引，并提取 c_code
                match parts.get(5) {
                    Some(candidate) if *candidate == SPECIAL_CITY_MARKER => {
                        // 特殊标记：尝试使用索引6作为真正的城市 code
                        city_index = 6;
                        c_code = parts.get(6).map(|s| s.to_string());
                    }
                    Some(candidate) => {
                        city_index = 5;
                        c_code = Some(candidate.to_string());
                    }
                    None => {
                        // 索引5不存在，保持默认 city_index = 5，c_code = None
                        c_code = None;
                    }
                }
            }

            let city_name = org
                .full_path_name
                .as_ref()
                .map(|path| {
                    let parts: Vec<&str> = path.split('-').collect();
                    parts
                        .get(city_index)
                        .map(|s| get_city_clean_re().replace_all(s.trim(), "").to_string())
                })
                .flatten();

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
                .push_bind(cleaned_name)
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
                .push_bind(c_code)
                .push_bind(province_name)
                .push_bind(p_code)
                .push_bind(city_name)
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
                .push_bind(None::<String>) // amount 设为 NULL
                .push_bind(org.full_path_id)
                .push_bind(org.full_path_name);
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
            IS_DELETE,
            full_path_id,
            full_path_name
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
                .push_bind(org_tree.is_delete)
                .push_bind(org_tree.full_path_id)
                .push_bind(org_tree.full_path_name);
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
            "Deleted {} records in table {}",
            result.rows_affected(),
            table_name
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
                    // 解构时，org 是 &Box<TelecomOrg> 类型
                    ProcessingState::GotTelecomOrg(log, _) => {
                        self.handle_got_telecom_org_state(log.clone()).await
                    }
                    ProcessingState::GotOrgTree(log, _) => {
                        self.handle_got_org_tree_state(log.clone()).await
                    }
                    ProcessingState::GotMssMapping(log, _, mss_code) => {
                        self.handle_got_mss_mapping_state(log.clone(), mss_code.clone())
                            .await
                    }
                };

                match next_transition_result {
                    // 状态成功推进
                    Ok(Transition::Advanced(next_state_box)) => {
                        // next_state_box 是 Box<ProcessingState>
                        // 核心逻辑：立即处理上一个状态的数据
                        match &*next_state_box {
                            // 使用 * 解引用 Box
                            ProcessingState::GotTelecomOrg(log, org) => {
                                // 从 Initial -> GotTelecomOrg，处理 org
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                // org 是 &Box<TelecomOrg>，使用 .id 会自动解引用
                                processed_data.org_ids_to_delete.push(org.id.clone());
                                if need_insert {
                                    // (**org) 从 &Box<T> 得到 T
                                    let mut org_to_insert = (**org).clone();
                                    org_to_insert.year = Some(year.clone());
                                    org_to_insert.month = Some(month.clone());
                                    org_to_insert.in_time = Some(now);
                                    org_to_insert.hit_date1 = Some(now);
                                    org_to_insert.hit_date =
                                        Some(now.format("%Y-%m-%d").to_string());
                                    processed_data.telecom_orgs.push(org_to_insert);
                                }
                            }
                            ProcessingState::GotOrgTree(log, tree) => {
                                // 从 GotTelecomOrg -> GotOrgTree，处理 tree
                                let need_insert = log.type_ == 1 || log.type_ == 2;
                                processed_data.org_tree_ids_to_delete.push(tree.id.clone());
                                if need_insert {
                                    processed_data.telecom_org_trees.push((**tree).clone());
                                }
                            }
                            ProcessingState::GotMssMapping(log, mapping, mss_code) => {
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
                        // 更新状态，从 Box 中移出值
                        current_state = *next_state_box;
                    }
                    // 所有步骤都已成功完成
                    Ok(Transition::Completed(log, mss_orgs)) => {
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
        info!(
            "states_for_retry: {states_for_retry:?} len: {}",
            states_for_retry.len()
        );
        (processed_data, states_for_retry, permanent_failures)
    }

    // --- 为每个状态创建一个独立的辅助处理函数，使逻辑更清晰 ---
    async fn handle_initial_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        match self.transform_to_telecom_org(&log).await? {
            // 成功获取，返回 Advanced 状态
            Some(org) => Ok(Transition::Advanced(Box::new(
                ProcessingState::GotTelecomOrg(log, Box::new(org)),
            ))),
            None => Err(ProcessError::Permanent(anyhow::anyhow!(
                "Unable to find corresponding TelecomOrg"
            ))),
        }
    }

    async fn handle_got_telecom_org_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        match self.transform_to_org_tree(&log).await? {
            Some(tree) => Ok(Transition::Advanced(Box::new(ProcessingState::GotOrgTree(
                log,
                Box::new(tree),
            )))),
            None => Err(ProcessError::Permanent(anyhow::anyhow!(
                "Unable to generate OrgTree"
            ))),
        }
    }

    async fn handle_got_org_tree_state(
        &self,
        log: ModifyOperationLog,
    ) -> Result<Transition, ProcessError> {
        let (mapping, mss_code) = self.transform_to_mss_org_mapping(&log).await?;
        // 成功获取，返回 Advanced 状态
        Ok(Transition::Advanced(Box::new(
            ProcessingState::GotMssMapping(log, mapping, mss_code),
        )))
    }

    async fn handle_got_mss_mapping_state(
        &self,
        log: ModifyOperationLog,
        mss_code: String,
    ) -> Result<Transition, ProcessError> {
        let mss_orgs = self
            .transform_to_mss_orgs(&mss_code)
            .await?
            .ok_or_else(|| {
                ProcessError::Permanent(anyhow::anyhow!("Unable to find TelecomMssOrg"))
            })?;

        // 这是最后一步，成功后返回 Completed 状态，并携带所有数据
        Ok(Transition::Completed(Box::new(log), mss_orgs))
    }
}

#[test]
fn test_city_clean() {
    let inputs = [
        ("盐城分公司 ", "盐城"),
        ("中国电信股份有限公司 新乡分公司", "新乡"),
        ("晋城市 电信分公司* ", "晋城"),
    ];
    for (input, expected) in inputs {
        let cleaned = get_city_clean_re().replace_all(input, "").to_string();
        assert_eq!(cleaned, expected);
    }
}
