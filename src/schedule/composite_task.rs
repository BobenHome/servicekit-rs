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
    fn execute(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        let tasks_clone = self.tasks.clone();
        let task_name_clone = self.task_name.clone();

        Box::pin(async move {
            info!("********定时任务 '{}' 开始。", task_name_clone);
            for (i, task) in tasks_clone.iter().enumerate() {
                let current_task_name =
                    format!("子任务 #{} ({}的第{}个任务)", i + 1, task_name_clone, i + 1);
                info!("  开始执行 {}", current_task_name); // 缩进表示子任务
                if let Err(e) = task.execute().await {
                    error!("  执行 {} 异常: {:?}", current_task_name, e);
                    // 在这里可以选择：
                    // 1. 直接返回 Err，中断后续任务 (类似 Java 抛出异常中断)
                    // return Err(e);
                    // 2. 记录错误，但继续执行后续任务 (类似 Java try-catch 内部)
                    // 为了更像 Spring 的 try-catch 行为，我们选择继续，但记录错误
                } else {
                    info!("  {} 执行成功。", current_task_name);
                }
            }
            info!("********定时任务 '{}' 结束。", task_name_clone);
            Ok(())
        })
    }
}
