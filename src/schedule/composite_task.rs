use crate::TaskExecutor;
use std::sync::Arc;
use tracing::{error, info};

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

#[async_trait::async_trait]
impl TaskExecutor for CompositeTask {
    fn name(&self) -> &str {
        &self.task_name
    }

    async fn execute(&self) -> anyhow::Result<()> {
        let task_name = &self.task_name;
        let tasks_len = self.tasks.len();

        info!("Composite task '{task_name}' started. Containing {tasks_len} subtasks.");
        for (idx, subtask) in self.tasks.iter().enumerate() {
            let sub_name = subtask.name();
            info!("Starting subtask {}/{tasks_len}: '{sub_name}'.", idx + 1);
            match subtask.execute().await {
                Ok(_) => info!("Subtask '{sub_name}' completed successfully."),
                Err(e) => error!("Subtask '{sub_name}' failed: {e:?}"),
            }
        }
        info!("Composite task '{task_name}' finished.");
        Ok(())
    }
}
