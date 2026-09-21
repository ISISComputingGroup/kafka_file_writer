/// Define which Kafka keys this writer should be notified for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionKey {
    /// Deliver messages from all Kafka keys.
    All,
    /// Only deliver messages which have a Kafka key matching
    /// the specified bytes.
    Key(Vec<u8>),
}

/// Writer-declared subscription to a Kafka topic and key.
/// The writer will be notified of messages delivered to this
/// topic + key combination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    topic: String,
    key: SubscriptionKey,
}

impl Subscription {
    pub fn new(topic: impl Into<String>, key: SubscriptionKey) -> Subscription {
        Subscription {
            topic: topic.into(),
            key,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn key(&self) -> &SubscriptionKey {
        &self.key
    }
}
