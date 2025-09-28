use crate::config::TasksConfig;
use crate::schedule::binlog_sync::BinlogSyncTask;
use crate::{
    AppContext, TaskExecutor,
    schedule::{
        CompositeTask, PsnArchivePushTask, PsnArchiveScPushTask, PsnClassPushTask,
        PsnClassScPushTask, PsnLecturerPushTask, PsnLecturerScPushTask, PsnTrainingPushTask,
        PsnTrainingScPushTask,
    },
};
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

pub struct TaskSchedulerManager {
    scheduler: JobScheduler,
}

impl TaskSchedulerManager {
    pub async fn new() -> Result<Self> {
        // 初始化任务调度器
        // --- 使用 tokio-cron-scheduler 启动调度器 ---
        let scheduler = JobScheduler::new()
            .await
            .context("Failed to create scheduler")?;
        info!("Scheduler initialized.");
        Ok(Self { scheduler })
    }

    pub async fn start(self) {
        tokio::spawn(async move {
            if let Err(e) = self.scheduler.start().await {
                error!("Failed to start scheduler in background: {e:?}");
            } else {
                info!("Scheduler successfully started in background.");
            }
        });
    }

    pub async fn initialize_tasks(
        &self,
        app_context: Arc<AppContext>,
        tasks_config: &TasksConfig,
    ) -> Result<()> {
        // 创建所有推送任务实例
        let tasks = self.create_push_tasks(&app_context);

        // 创建复合任务
        let composite_task = Arc::new(CompositeTask::new(
            tasks,
            tasks_config.psn_push.task_name.clone(),
        ));

        // 使用辅助函数创建并添加 CompositeTask 的 Cron Job
        // 添加到调度器
        self.create_schedule_job(
            composite_task, // Arc<CompositeTask> 会自动转换为 Arc<dyn TaskExecutor>
            tasks_config.psn_push.cron_schedule.as_str(),
            vec![],
        )
        .await?;

        // --- 连续任务 ---
        // 1. 创建 BinlogSyncTask 实例
        let binlog_task = Arc::new(BinlogSyncTask::new(Arc::clone(&app_context)));

        // 2. 将其作为连续任务启动，而不是 Cron Job
        self.run_continuous_task(binlog_task).await;

        Ok(())
    }

    fn create_push_tasks(
        &self,
        app_context: &Arc<AppContext>,
    ) -> Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> {
        vec![
            Arc::new(PsnClassPushTask::new(Arc::clone(app_context), None, None)),
            Arc::new(PsnLecturerPushTask::new(
                Arc::clone(app_context),
                None,
                None,
            )),
            Arc::new(PsnArchivePushTask::new(Arc::clone(app_context), None, None)),
            Arc::new(PsnTrainingPushTask::new(
                Arc::clone(app_context),
                None,
                None,
            )),
            Arc::new(PsnClassScPushTask::new(Arc::clone(app_context), None, None)),
            Arc::new(PsnLecturerScPushTask::new(
                Arc::clone(app_context),
                None,
                None,
            )),
            Arc::new(PsnArchiveScPushTask::new(
                Arc::clone(app_context),
                None,
                None,
            )),
            Arc::new(PsnTrainingScPushTask::new(
                Arc::clone(app_context),
                None,
                None,
            )),
        ]
    }

    // 辅助函数：创建并调度一个任务的 Cron Job
    async fn create_schedule_job(
        &self,
        primary_task: Arc<dyn TaskExecutor + Send + Sync + 'static>, // 主任务
        cron_schedule: &str,
        dependent_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>, // 依赖任务
    ) -> Result<()> {
        let primary_task_clone = Arc::clone(&primary_task);
        let job_name = primary_task_clone.name().to_string();

        let job = Job::new_async_tz(
            cron_schedule,
            chrono_tz::Asia::Shanghai,
            move |uuid, _scheduler| {
                let task = Arc::clone(&primary_task_clone);
                let job_name_future = task.name().to_string();
                let deps = dependent_tasks.clone();

                Box::pin(async move {
                    info!("Job '{job_name_future}' ({uuid:?}) is running.");
                    // --- 执行主任务 ---
                    if let Err(e) = task.execute().await {
                        error!("Error executing primary job '{job_name_future}' {uuid:?}: {e:?}");
                    } else {
                        info!("Primary job '{job_name_future}' ({uuid:?}) completed successfully.");
                        // --- 执行依赖任务 ---
                        Self::execute_dependent_tasks(&job_name_future, deps).await;
                    }
                })
            },
        )
        .context(format!("Failed to create cron job '{job_name}'"))?;

        self.scheduler
            .add(job)
            .await
            .context(format!("Failed to add job '{job_name}' to scheduler"))?;
        info!("Job '{job_name}' added to scheduler.");

        Ok(())
    }

    /// 启动一个在后台持续运行的任务
    async fn run_continuous_task(&self, task: Arc<BinlogSyncTask>) {
        let task_name = task.name().to_string();
        info!("Spawning continuous task '{task_name}' to run in the background.");

        tokio::spawn(async move {
            let idle_sleep = Duration::from_secs(60); // 空闲时休眠60秒
            let busy_sleep = Duration::from_secs(1); // 追赶时休眠1秒
            let error_sleep = Duration::from_secs(10); // 出错时休眠10秒

            loop {
                info!("Starting a new cycle for continuous task '{task_name}'.");

                match task.sync_data().await {
                    Ok(true) => {
                        // binlog 日志追赶上系统时间后，休眠60s后再执行
                        info!("System is caught up. Sleeping for {idle_sleep:?}.");
                        sleep(idle_sleep).await;
                    }
                    Ok(false) => {
                        //  成功后短暂休眠，避免对数据库或API造成过大压力
                        info!("Continuous task '{task_name}' completed a cycle successfully.");
                        info!("System is catching up. Sleeping for {busy_sleep:?}.");
                        sleep(busy_sleep).await;
                    }
                    Err(e) => {
                        error!(
                            "Continuous task '{task_name}' failed: {e:?}. Waiting for 10 seconds before next cycle."
                        );
                        // 如果任务失败，等待一段时间再重试，避免因连续失败导致CPU空转或频繁攻击下游服务
                        sleep(error_sleep).await;
                    }
                }
            }
        });
    }

    async fn execute_dependent_tasks(
        primary_job_name: &str,
        deps: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>,
    ) {
        if deps.is_empty() {
            info!("No dependent tasks to execute for '{primary_job_name}'.");
            return;
        }

        info!(
            "Starting {} dependent tasks for '{primary_job_name}'.",
            deps.len()
        );
        // --- 遍历并执行所有依赖任务 ---
        for (i, task) in deps.iter().enumerate() {
            let task_num = i + 1;
            info!("Executing dependent task #{task_num} for '{primary_job_name}'.");

            match task.execute().await {
                Ok(()) => {
                    info!(
                        "Dependent task #{task_num} for '{primary_job_name}' completed successfully."
                    );
                }
                Err(e) => {
                    error!(
                        "Error executing dependent task #{task_num} for '{primary_job_name}': {e:?}"
                    );
                }
            }
        }
    }
}
