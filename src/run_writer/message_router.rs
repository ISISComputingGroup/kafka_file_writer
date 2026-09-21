//! Message router: routes messages by topic and key to writer modules that declared
//! those as subscriptions.
//!
//! Naively, we could simply pass every message to every writer module, and writer
//! modules would be responsible for ignoring irrelevant messages. However,
//! the naive approach is not performant for the case where we have many IXseblock
//! writers and corresponding f144 messages (it would scale as N^2)
//!
//! Instead, allow writer modules to subscribe to topics+keys specified in advance.
//! Use a hashmap lookup to determine the appropriate writer(s) for this key. Writers
//! may subscribe to all keys if they need to receive all messages from a topic.
use crate::stream::traits::KafkaMessageMeta;
use crate::subscription::{Subscription, SubscriptionKey};
use ahash::HashMap;

/// Unique identifier for a specific writer module (which might, for example,
/// be an index into some external `Vec`).
pub type WriterId = usize;

/// Message router for all topics.
#[derive(Debug, Default)]
pub struct MessageRouter {
    /// Map of topic to topic-router for that topic.
    topic_routers: HashMap<String, TopicMessageRouter>,

    /// Router used for topics to which nothing has subscribed.
    default_router: TopicMessageRouter,
}

/// Message router for a single topic.
///
/// The type-parameter T is a type uniquely identifying a specific
/// writer module.
#[derive(Debug, Default)]
struct TopicMessageRouter {
    keyed_subscribers: HashMap<Vec<u8>, Vec<WriterId>>,
    all_key_subscribers: Vec<WriterId>,
}

impl MessageRouter {
    /// Subscribe to receive messages described by a ``Subscription``.
    pub fn subscribe(&mut self, id: WriterId, subscription: &Subscription) {
        self.topic_routers
            .entry(subscription.topic().to_owned())
            .or_default()
            .subscribe(id, subscription)
    }

    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.topic_routers.keys().map(AsRef::as_ref)
    }

    /// Get the instances of ``T`` that should be notified for the specified message metadata.
    pub fn writers_for(&self, meta: &KafkaMessageMeta) -> impl Iterator<Item = WriterId> {
        self.topic_routers
            .get(meta.topic)
            .unwrap_or(&self.default_router)
            .writers_for(meta)
    }
}

impl TopicMessageRouter {
    fn subscribe(&mut self, id: WriterId, subscription: &Subscription) {
        match subscription.key() {
            SubscriptionKey::Key(key) => self
                .keyed_subscribers
                .entry(key.clone())
                .or_default()
                .push(id),
            SubscriptionKey::All => {
                self.all_key_subscribers.push(id);
            }
        }
    }

    fn writers_for(&self, meta: &KafkaMessageMeta) -> impl Iterator<Item = WriterId> {
        meta.key
            .and_then(|k| self.keyed_subscribers.get(k))
            .map(|s| s.as_slice())
            .unwrap_or(&[])
            .iter()
            .chain(self.all_key_subscribers.iter())
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::HashSet;
    use std::hash::Hash;

    fn meta<'a>(topic: &'a str, key: Option<&'a [u8]>) -> KafkaMessageMeta<'a> {
        KafkaMessageMeta {
            topic,
            key,
            partition: 0,
            offset: 0,
            timestamp: Some(0),
        }
    }

    fn to_set<T: Hash + Eq>(v: impl IntoIterator<Item = T>) -> HashSet<T> {
        v.into_iter().collect()
    }

    #[test]
    fn test_unknown_topic() {
        let mut router = MessageRouter::default();

        router.subscribe(123, &Subscription::new("someTopic", SubscriptionKey::All));

        let writers = router
            .writers_for(&meta("someOtherTopic", Some(b"")))
            .collect::<Vec<_>>();

        assert!(writers.is_empty());
    }

    #[test]
    fn test_known_topic_all_keys() {
        let mut router = MessageRouter::default();

        router.subscribe(123, &Subscription::new("someTopic", SubscriptionKey::All));

        let writers = router
            .writers_for(&meta("someTopic", Some(b"")))
            .collect::<Vec<_>>();

        assert_eq!(writers, vec![123]);
    }

    #[test]
    fn test_known_topic_unknown_key() {
        let mut router = MessageRouter::default();

        router.subscribe(
            123,
            &Subscription::new("someTopic", SubscriptionKey::Key(b"foo".to_vec())),
        );

        let writers = router
            .writers_for(&meta("someTopic", Some(b"a_different_key")))
            .collect::<Vec<_>>();

        assert!(writers.is_empty());
    }

    #[test]
    fn test_known_topic_known_key() {
        let mut router = MessageRouter::default();

        router.subscribe(
            123,
            &Subscription::new("someTopic", SubscriptionKey::Key(b"someKey".to_vec())),
        );

        let writers = router
            .writers_for(&meta("someTopic", Some(b"someKey")))
            .collect::<Vec<_>>();

        assert_eq!(writers, vec![123]);
    }

    #[test]
    fn test_multiple_subscriptions() {
        let mut router = MessageRouter::default();

        router.subscribe(1, &Subscription::new("topic1", SubscriptionKey::All));
        router.subscribe(
            2,
            &Subscription::new("topic1", SubscriptionKey::Key(b"key2".to_vec())),
        );
        router.subscribe(
            3,
            &Subscription::new("topic1", SubscriptionKey::Key(b"key3".to_vec())),
        );
        router.subscribe(
            4,
            &Subscription::new("topic1", SubscriptionKey::Key(b"key3".to_vec())),
        );
        router.subscribe(11, &Subscription::new("topic2", SubscriptionKey::All));
        router.subscribe(
            12,
            &Subscription::new("topic2", SubscriptionKey::Key(b"key2".to_vec())),
        );
        router.subscribe(
            13,
            &Subscription::new("topic2", SubscriptionKey::Key(b"key3".to_vec())),
        );
        router.subscribe(
            14,
            &Subscription::new("topic2", SubscriptionKey::Key(b"key3".to_vec())),
        );

        assert_eq!(
            router
                .writers_for(&meta("topic1", Some(b"key1")))
                .collect::<HashSet<_>>(),
            to_set([1])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic1", None))
                .collect::<HashSet<_>>(),
            to_set([1])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic1", Some(b"key2")))
                .collect::<HashSet<_>>(),
            to_set([1, 2])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic1", Some(b"key3")))
                .collect::<HashSet<_>>(),
            to_set([1, 3, 4])
        );

        assert_eq!(
            router
                .writers_for(&meta("topic2", Some(b"key1")))
                .collect::<HashSet<_>>(),
            to_set([11])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic2", None))
                .collect::<HashSet<_>>(),
            to_set([11])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic2", Some(b"key2")))
                .collect::<HashSet<_>>(),
            to_set([11, 12])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic2", Some(b"key3")))
                .collect::<HashSet<_>>(),
            to_set([11, 13, 14])
        );

        assert_eq!(
            router
                .writers_for(&meta("topic3", Some(b"key1")))
                .collect::<HashSet<_>>(),
            to_set([])
        );
        assert_eq!(
            router
                .writers_for(&meta("topic3", None))
                .collect::<HashSet<_>>(),
            to_set([])
        );
    }
}
