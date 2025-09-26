use crate::schedule::binlog_sync::{ModifyOperationLog, PermanentFailure};
use crate::utils::ProcessError;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{Local, NaiveDateTime};
use std::fmt::Debug;
use tracing::{error, info};

// 最大重试次数
const MAX_RETRIES: u32 = 3;

// 共享 trait 用于 ProcessedData 的 merge
pub trait MergeableProcessedData {
    fn merge(&mut self, other: &mut Self);
}

/// 定义处理状态机，用于保存每个日志的处理进度
// 泛型 ProcessingState：Intermediate1 (e.g., Org/User), Intermediate2 (e.g., Tree or ()), Mapping (e.g., MssMapping)
#[derive(Debug)]
pub enum ProcessingState<I1, I2, M> {
    Initial(ModifyOperationLog),
    GotStep1(ModifyOperationLog, Box<I1>), // Box 优化大结构体大小，将大的字段（如 TelecomOrg）包装在 Box 里，让枚举变体本身变得非常小，从而让整个枚举都变得小巧
    GotStep2(ModifyOperationLog, Box<I2>),
    GotMapping(ModifyOperationLog, M, String), // String 为 mss_code 或者 hrCode
}

// 泛型 Transition 表示状态转换的结果
#[derive(Debug)]
pub enum Transition<I1, I2, M, F> {
    Advanced(Box<ProcessingState<I1, I2, M>>),
    Completed(Box<ModifyOperationLog>, Vec<F>), // F 为最终数据 e.g., Vec<TelecomMssOrg>
}

#[async_trait]
pub trait DataProcessorTrait: Send + Sync {
    type ProcessedData: Default + MergeableProcessedData + Send;
    type Intermediate1: Clone + Send + Debug; // e.g., TelecomOrg
    type Intermediate2: Clone + Send + Debug; // e.g., TelecomOrgTree or ()
    type Mapping: Clone + Send + Debug; // e.g., TelecomMssOrgMapping
    type Final: Clone + Send + Debug; // e.g., TelecomMssOrg

    // 每个步骤的 handle 函数，由具体处理器实现
    async fn handle_initial(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<
        Transition<Self::Intermediate1, Self::Intermediate2, Self::Mapping, Self::Final>,
        ProcessError,
    >;

    async fn handle_step1(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<
        Transition<Self::Intermediate1, Self::Intermediate2, Self::Mapping, Self::Final>,
        ProcessError,
    >;

    async fn handle_step2(
        &self,
        log: &ModifyOperationLog,
    ) -> Result<
        Transition<Self::Intermediate1, Self::Intermediate2, Self::Mapping, Self::Final>,
        ProcessError,
    >;

    async fn handle_mapping(
        &self,
        log: &ModifyOperationLog,
        mss_code: &str,
    ) -> Result<
        Transition<Self::Intermediate1, Self::Intermediate2, Self::Mapping, Self::Final>,
        ProcessError,
    >;

    // 钩子：处理 Advanced 时的数据累积，由具体实现定义（e.g., 添加 org 到 processed_data，设置 year/month）
    fn post_advance(
        &self,
        data: &mut Self::ProcessedData,
        state: &ProcessingState<Self::Intermediate1, Self::Intermediate2, Self::Mapping>,
        year: &str,
        month: &str,
        now: NaiveDateTime,
    );

    // 钩子：处理 Completed 时的数据累积
    fn post_complete(
        &self,
        data: &mut Self::ProcessedData,
        log: &ModifyOperationLog,
        final_data: Vec<Self::Final>,
        year: &str,
        month: &str,
        now: NaiveDateTime,
    );

    // 共享的 advance_states 函数（可作为 trait 方法调用）
    async fn advance_states(
        &self,
        states: Vec<ProcessingState<Self::Intermediate1, Self::Intermediate2, Self::Mapping>>,
    ) -> (
        Self::ProcessedData,
        Vec<ProcessingState<Self::Intermediate1, Self::Intermediate2, Self::Mapping>>,
        Vec<PermanentFailure>,
    ) {
        let mut processed_data = Self::ProcessedData::default();
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
                    ProcessingState::Initial(log) => self.handle_initial(log).await,
                    ProcessingState::GotStep1(log, _) => self.handle_step1(log).await,
                    ProcessingState::GotStep2(log, _) => self.handle_step2(log).await,
                    ProcessingState::GotMapping(log, _, mss_code) => {
                        self.handle_mapping(log, mss_code).await
                    }
                };

                match next_transition_result {
                    // 状态成功推进
                    Ok(Transition::Advanced(next_state_box)) => {
                        // 调用钩子处理数据
                        // 核心逻辑：立即处理上一个状态的数据
                        self.post_advance(&mut processed_data, &next_state_box, &year, &month, now);
                        // 更新状态，继续循环
                        // 更新状态，从 Box 中移出值
                        current_state = *next_state_box;
                    }
                    // 所有步骤都已成功完成
                    Ok(Transition::Completed(log, final_data)) => {
                        // 调用钩子处理最终数据
                        self.post_complete(
                            &mut processed_data,
                            &log,
                            final_data,
                            &year,
                            &month,
                            now,
                        );
                        break; // 此日志处理完成，跳出 loop
                    }
                    Err(ProcessError::GatewayTimeout(_)) => {
                        // 发生超时，将当前状态加入重试列表
                        states_for_retry.push(current_state);
                        break;
                    }
                    Err(ProcessError::Permanent(e)) => {
                        // 发生永久性错误，记录并放弃
                        let log = extract_log_from_state(current_state);
                        permanent_failures.push(PermanentFailure {
                            log,
                            reason: e.to_string(),
                        });
                        break;
                    }
                }
            }
        }
        info!(
            "states_for_retry: {:?} len: {}",
            states_for_retry,
            states_for_retry.len()
        );
        (processed_data, states_for_retry, permanent_failures)
    }

    // 新增：保存处理数据的抽象方法
    async fn save_processed_data(&self, data: &Self::ProcessedData) -> Result<()>;

    // 新增：刷新表的抽象方法
    async fn refresh_table(&self, data: &Self::ProcessedData) -> Result<()>;

    // 默认实现的 process 方法，主入口函数，包含了重试逻辑
    async fn process(&self, logs: Vec<ModifyOperationLog>) -> Result<()> {
        // 初始化状态机
        let mut states_to_process: Vec<
            ProcessingState<Self::Intermediate1, Self::Intermediate2, Self::Mapping>,
        > = logs.into_iter().map(ProcessingState::Initial).collect();

        let mut final_processed_data = Self::ProcessedData::default();

        for i in 0..MAX_RETRIES {
            if states_to_process.is_empty() {
                info!("All data has been successfully processed.");
                break;
            }
            info!(
                "Processing data, {} retry attempts remaining. Pending count: {}",
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
            Ok(_) => info!("All batches of data successfully saved to database."),
            Err(e) => error!("Failed to save data: {e:?}"),
        }

        // 在 d_* 表更新成功后，刷新 mc_user_ztk 或者 mc_org_show 表
        if let Err(e) = self.refresh_table(&final_processed_data).await {
            error!("Failed to refresh table: {e:?}");
        }

        Ok(())
    }
}

// 辅助函数：提取 log（共享）
fn extract_log_from_state<I1, I2, M>(state: ProcessingState<I1, I2, M>) -> ModifyOperationLog {
    match state {
        ProcessingState::Initial(log) => log,
        ProcessingState::GotStep1(log, _) => log,
        ProcessingState::GotStep2(log, _) => log,
        ProcessingState::GotMapping(log, _, _) => log,
    }
}
