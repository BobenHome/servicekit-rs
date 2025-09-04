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
        let task_name = self.task_name.clone();

        Box::pin(async move {
            let tasks_len = tasks_clone.len();
            info!("Composite task '{task_name}' started. Containing {tasks_len} subtasks.");
            for (idx, subtask) in tasks_clone.iter().enumerate() {
                let sub_name = subtask.name();
                info!("Starting subtask {}/{tasks_len}: '{sub_name}'.", idx + 1);
                match subtask.execute().await {
                    Ok(_) => info!("Subtask '{sub_name}' completed successfully."),
                    Err(e) => error!("Subtask '{sub_name}' failed: {e:?}"),
                }
            }
            info!("Composite task '{task_name}' finished.");
            Ok(())
        })
    }
}
