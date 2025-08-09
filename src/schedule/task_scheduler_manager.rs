use crate::{
    schedule::{
        CompositeTask, PsnArchivePushTask, PsnArchiveScPushTask, PsnLecturerPushTask,
        PsnLecturerScPushTask, PsnTrainPushTask, PsnTrainScPushTask, PsnTrainingPushTask,
        PsnTrainingScPushTask,
    },
    AppConfig, AppContext, TaskExecutor,
};
use anyhow::{Context, Result};
use chrono::Local;
use std::sync::Arc;
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
        app_config: &AppConfig,
    ) -> Result<()> {
        // 创建所有推送任务实例
        let tasks = self.create_push_tasks(app_context);

        // 创建复合任务
        let composite_task = Arc::new(CompositeTask::new(
            tasks,
            app_config.tasks.psn_push.task_name.clone(),
        ));

        // 使用辅助函数创建并添加 CompositeTask 的 Cron Job
        // 添加到调度器
        self.create_and_schedule_task_job(
            composite_task, // Arc<CompositeTask> 会自动转换为 Arc<dyn TaskExecutor>
            app_config.tasks.psn_push.cron_schedule.as_str(),
            vec![],
        )
        .await?;

        Ok(())
    }

    fn create_push_tasks(
        &self,
        app_context: Arc<AppContext>,
    ) -> Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>> {
        vec![
            Arc::new(PsnTrainPushTask::new(Arc::clone(&app_context), None, None)),
            Arc::new(PsnLecturerPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnArchivePushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnTrainingPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnTrainScPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnLecturerScPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnArchiveScPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
            Arc::new(PsnTrainingScPushTask::new(
                Arc::clone(&app_context),
                None,
                None,
            )),
        ]
    }

    // 这个函数接收一个实现了 TaskExecutor trait 的任务实例
    // 辅助函数：创建并调度一个任务的 Cron Job
    async fn create_and_schedule_task_job(
        &self,
        primary_task: Arc<dyn TaskExecutor + Send + Sync + 'static>, // 主任务
        cron_schedule: &str,
        dependent_tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>, // 依赖任务
    ) -> Result<()> {
        let primary_task_clone = Arc::clone(&primary_task);
        let job_name = primary_task_clone.name().to_string();

        // 克隆依赖任务（如果存在）给 Job 闭包
        let dependent_tasks_clone_for_job = dependent_tasks.clone();

        let job = Job::new_async_tz(cron_schedule,
            chrono_tz::Asia::Shanghai,move |uuid, mut scheduler| {
                let primary_task_for_future = Arc::clone(&primary_task_clone);
                let job_name_for_future = primary_task_for_future.name().to_string();
                // 克隆依赖任务给 inner async block
                let dependent_tasks_for_future = dependent_tasks_clone_for_job.clone();

                Box::pin(async move {
                    let next_scheduled_time_str = match scheduler.next_tick_for_job(uuid).await {
                        Ok(Some(dt)) => dt
                            .with_timezone(&Local)
                            .format("%Y-%m-%d %H:%M:%S")
                            .to_string(),
                        Ok(None) => "No next tick".to_string(),
                        Err(e) => {
                            error!("Error getting next tick for job {uuid:?}: {e:?}");
                            "Error getting next tick".to_string()
                        }
                    };
                    info!(
                        "Job '{job_name_for_future}' ({uuid:?}) is running. Next scheduled time (local): {next_scheduled_time_str}"
                    );

                    // --- 执行主任务 ---
                    if let Err(e) = primary_task_for_future.execute().await {
                        error!(
                            "Error executing primary job '{job_name_for_future}' {uuid:?}: {e:?}"
                        );
                    } else {
                        info!(
                            "Primary job '{job_name_for_future}' ({uuid:?}) completed successfully."
                        );
                        // --- 遍历并执行所有依赖任务 ---
                        if dependent_tasks_for_future.is_empty() {
                            info!(
                                "No dependent tasks to execute for '{job_name_for_future}'."
                            );
                        } else {
                            info!(
                                "Starting {} dependent tasks for '{job_name_for_future}'.",
                                dependent_tasks_for_future.len()
                            );
                            for (i, dep_task) in dependent_tasks_for_future.iter().enumerate() {
                                info!(
                                    "Executing dependent task #{} for '{job_name_for_future}'.",
                                    i + 1
                                );
                                // 依赖任务本身会打印其执行状态的日志
                                if let Err(e) = dep_task.execute().await {
                                    error!(
                                        "Error executing dependent task #{} for '{job_name_for_future}': {e:?}",
                                        i + 1
                                    );
                                    // 如果某个依赖任务失败，你可以选择是中断后续依赖任务，还是继续
                                    // 这里我们选择继续执行其他依赖任务，但会记录错误
                                } else {
                                    info!(
                                        "Dependent task #{} for '{job_name_for_future}' completed successfully.",
                                        i + 1
                                    );
                                }
                            }
                        }
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
}
