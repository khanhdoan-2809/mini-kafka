package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryMessageStore implements MessageStore {

    private final ConcurrentHashMap<String, TopicLog> topics =
            new ConcurrentHashMap<>();

    @Override
    public long append(String topic, String key, String value) {
        validateTopic(topic);
        TopicLog topicLog = topics.computeIfAbsent(topic, ignored -> new TopicLog());

        return topicLog.append(key, value);
    }

    @Override
    public List<BrokerRecord> fetch(String topic, long fromOffset, int maxRecords) {
        validateTopic(topic);

        if (fromOffset < 0) {
            throw new IllegalArgumentException(
                    "Offset must be >= 0"
            );
        }

        if (maxRecords <= 0) {
            throw new IllegalArgumentException(
                    "maxRecords must be > 0"
            );
        }

        var topicLog = topics.get(topic);

        if (topicLog == null) {
            return List.of();
        }

        return topicLog.fetch(fromOffset, maxRecords);
    }

    private void validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic must not be blank"
            );
        }
    }

    private static class TopicLog {

        private final List<BrokerRecord> records =
                new ArrayList<>();

        private final ReentrantLock lock =  new ReentrantLock();

        public long append(String key, String value) {
            lock.lock();
            try {
                var offset = records.size();

                var record = new BrokerRecord(offset, Instant.now().toEpochMilli(), key, value);
                records.add(record);

                return offset;
            } finally {
                lock.unlock();
            }
        }

        public synchronized List<BrokerRecord> fetch(long fromOffset,int maxRecords) {
            lock.lock();

            try {
                if (fromOffset >= records.size()) {
                    return List.of();
                }

                int startIndex = Math.toIntExact(fromOffset);

                int endIndex = Math.min(records.size(), startIndex + maxRecords);

                return List.copyOf(records.subList(startIndex, endIndex));
            } finally {
                lock.unlock();
            }
        }
    }
}