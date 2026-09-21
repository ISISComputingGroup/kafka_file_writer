//! Scoped assignments and subscriptions to Kafka
use crate::error::FileWriterError;
use crate::stream::traits::Stream;
use log::{debug, warn};
use miette::Report;
use std::time::Duration;

/// Run a closure while a consumer is assigned; unsubscribe on exit.
pub fn with_assignment<F, T>(
    consumer: &impl Stream,
    topics: &[&str],
    seek_timestamp: i64,
    timeout: Duration,
    func: F,
) -> Result<T, FileWriterError>
where
    F: FnOnce() -> Result<T, FileWriterError>,
{
    debug!("Assigning consumer to topics: {:?}", topics);
    consumer
        .assign_from_timestamp(topics, seek_timestamp, timeout)
        .map_err(|e| FileWriterError::from_stream_topic(e, &topics.join(",")))?;

    let result = func();

    if let Err(e) = consumer.unassign() {
        let report: Report = e.into();
        warn!("Unable to unassign assignment: {}", report);
    }
    result
}

/// Run a closure while a consumer is subscribed; unsubscribe on exit.
pub fn with_subscription_to_job_pool<F, T>(
    consumer: &impl Stream,
    job_pool_topic: &str,
    func: F,
) -> Result<T, FileWriterError>
where
    F: FnOnce() -> Result<T, FileWriterError>,
{
    consumer
        .subscribe(&[job_pool_topic])
        .map_err(|e| FileWriterError::from_stream_topic(e, job_pool_topic))?;
    let result = func();
    consumer.unsubscribe();
    result
}
