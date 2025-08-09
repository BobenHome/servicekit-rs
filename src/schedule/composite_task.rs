use std::{future::Future, pin::Pin, sync::Arc};

use tracing::{error, info};

use crate::TaskExecutor;

pub struct CompositeTask {
    tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>,
    pub task_name: String,
}

impl CompositeTask {
    pub fn new(
        tasks: Vec<Arc<dyn TaskExecutor + Send + Sync + 'static>>,
        task_name: String,
    ) -> Self {
        Self { tasks, task_name }
    }
}

impl TaskExecutor for CompositeTask {
    fn name(&self) -> &str {
        &self.task_name
    }

    fn execute(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let tasks_clone = self.tasks.clone();
        let task_name_clone = self.task_name.clone();

        Box::pin(async move {
            info!("--------任务 '{task_name_clone}' 开始。");
            for (i, task) in tasks_clone.iter().enumerate() {
                let current_task_name = format!(
                    "子任务 {} #{} ({}的第{}个任务)",
                    task.name(),
                    i + 1,
                    task_name_clone,
                    i + 1
                );
                info!("  开始执行 {current_task_name}"); // 缩进表示子任务
                if let Err(e) = task.execute().await {
                    error!("  执行 {current_task_name} 异常: {e:?}");
                } else {
                    info!("  {current_task_name} 执行成功。");
                }
            }
            info!("--------任务 '{task_name_clone}' 结束。");
            Ok(())
        })
    }
}
