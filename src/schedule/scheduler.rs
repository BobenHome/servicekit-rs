use crate::schedule::task::MySchedule;
use chrono::Local;
use cron::Schedule;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub struct Scheduler {
    tasks: Vec<Arc<dyn MySchedule + Send + Sync>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler { tasks: Vec::new() }
    }

    pub fn add_task<T>(&mut self, task: T)
    where
        T: MySchedule + Send + Sync + 'static,
    {
        self.tasks.push(Arc::new(task));
    }

    pub async fn start(&self) {
        for task in &self.tasks {
            let task = Arc::clone(task);
            tokio::spawn(async move {
                // 等待到首次执行时间
                if let Some(delay) = Self::get_initial_delay(task.cron_expression()) {
                    println!("Waiting for {} seconds before first execution", delay.as_secs());
                    tokio::time::sleep(delay).await;
                }

                loop {
                    task.execute().await;
                    tokio::time::sleep(Self::parse_duration(task.cron_expression())).await;
                }
            });
        }
    }

    fn get_initial_delay(cron: &str) -> Option<Duration> {
        if let Ok(schedule) = Schedule::from_str(cron) {
            let now = Local::now();
            if let Some(next_time) = schedule.upcoming(Local).next() {
                // 如果当前时间大于今天的执行时间，就等到明天的执行时间
                let duration = next_time.signed_duration_since(now);
                if duration.num_seconds() > 0 {
                    return Some(Duration::from_secs(duration.num_seconds() as u64));
                }
            }
        }
        None
    }

    fn parse_duration(cron: &str) -> Duration {
        match Self::validate_and_parse_cron(cron) {
            Ok(duration) => duration,
            Err(e) => {
                eprintln!(
                    "Cron expression error: {}. Falling back to default interval.",
                    e
                );
                Duration::from_secs(60)
            }
        }
    }

    fn validate_and_parse_cron(cron: &str) -> Result<Duration, Box<dyn std::error::Error>> {
        // 尝试解析 cron 表达式
        let schedule = Schedule::from_str(cron).unwrap();

        let now = Local::now();
        let next_time = schedule
            .upcoming(Local)
            .next()
            .ok_or("Could not determine next execution time")?;

        let duration = next_time.signed_duration_since(now);

        if duration.num_seconds() <= 0 {
            let next_next_time = schedule
                .upcoming(Local)
                .nth(1)
                .ok_or("Could not determine next execution time")?;

            Ok(Duration::from_secs(
                next_next_time
                    .signed_duration_since(now)
                    .num_seconds()
                    .max(0) as u64,
            ))
        } else {
            Ok(Duration::from_secs(duration.num_seconds() as u64))
        }
    }
}
