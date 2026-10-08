//! Scoped assignments and subscriptions to Kafka
use crate::error::FileWriterError;
use crate::stream::traits::Stream;
use log::{debug, warn};
use miette::Report;
use std::time::Duration;

/// Run a closure while a consumer is assigned; unassign on exit.
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
        .map_err(FileWriterError::StreamError)?;

    let result = func();

    let unassign_result = consumer.unassign();

    match (result, unassign_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Ok(_), Err(unassign_err)) => Err(unassign_err.into()),
        (Err(func_err), Ok(())) => Err(func_err),
        (Err(func_err), Err(unassign_err)) => {
            // We got *both* an error in the underlying function and an error on unassignment.
            // The 'primary' error is the one from the wrapped function. Additionally log the
            // unassignment failure.
            let report: Report = unassign_err.into();
            warn!(
                "While unassigning after an error, unassign() failed with: {}",
                report
            );
            Err(func_err)
        }
    }
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
        .map_err(FileWriterError::StreamError)?;

    let result = func();
    consumer.unsubscribe();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::error::StreamError;
    use crate::stream::fake::FakeStream;

    #[test]
    fn test_with_subscription_to_job_pool() {
        let stream = FakeStream::default();

        let result = with_subscription_to_job_pool(&stream, "runInfo", || {
            assert!(*stream.is_assigned.borrow());
            Ok("it worked")
        });

        assert_eq!(result.unwrap(), "it worked");
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_subscription_to_job_pool_func_failed() {
        let stream = FakeStream::default();

        let result: Result<(), _> = with_subscription_to_job_pool(&stream, "runInfo", || {
            assert!(*stream.is_assigned.borrow());
            Err(FileWriterError::UnitTestError)
        });

        assert!(result.is_err_and(|err| { matches!(err, FileWriterError::UnitTestError) }));
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_subscription_to_job_pool_subscription_failed() {
        let stream = FakeStream {
            subscribe_causes_error: true,
            ..Default::default()
        };

        let result: Result<(), _> = with_subscription_to_job_pool(&stream, "runInfo", || {
            panic!("should not have run closure if subscription failed")
        });

        assert!(result.is_err_and(|err| {
            matches!(
                err,
                FileWriterError::StreamError(StreamError::AssignmentError { .. })
            )
        }));
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_assignment_to_job_pool() {
        let stream = FakeStream::default();

        let result = with_assignment(
            &stream,
            &["events"],
            123,
            Duration::from_millis(100),
            || {
                assert!(*stream.is_assigned.borrow());
                Ok("it worked")
            },
        );

        assert_eq!(result.unwrap(), "it worked");
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_assignment_to_job_pool_func_failed() {
        let stream = FakeStream::default();

        let result: Result<(), _> = with_assignment(
            &stream,
            &["events"],
            123,
            Duration::from_millis(100),
            || {
                assert!(*stream.is_assigned.borrow());
                Err(FileWriterError::UnitTestError)
            },
        );

        assert!(result.is_err_and(|err| { matches!(err, FileWriterError::UnitTestError) }));
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_assignment_to_job_pool_assignment_failed() {
        let stream = FakeStream {
            assign_from_timestamp_causes_error: true,
            ..Default::default()
        };

        let result: Result<(), _> = with_assignment(
            &stream,
            &["events"],
            123,
            Duration::from_millis(100),
            || panic!("should not have run if assignment failed"),
        );

        assert!(result.is_err_and(|err| {
            matches!(
                err,
                FileWriterError::StreamError(StreamError::AssignmentError { .. })
            )
        }));
        assert!(!*stream.is_assigned.borrow());
    }

    #[test]
    fn test_with_assignment_to_job_pool_unassign_failed() {
        let stream = FakeStream {
            unassign_causes_error: true,
            ..Default::default()
        };

        let result = with_assignment(
            &stream,
            &["events"],
            123,
            Duration::from_millis(100),
            || Ok("it worked"),
        );

        assert!(result.is_err_and(|err| {
            matches!(
                err,
                FileWriterError::StreamError(StreamError::AssignmentError { .. })
            )
        }));
    }
}
