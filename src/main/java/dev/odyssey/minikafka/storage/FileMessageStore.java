package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class FileMessageStore implements MessageStore {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    private final Path dataDirectory;
    private final ConcurrentHashMap<String, FileTopicLog> topics = new ConcurrentHashMap<>();
    private final ReentrantLock topicRegistryLock = new ReentrantLock();

    public FileMessageStore(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        Files.createDirectories(dataDirectory);
    }

    @Override
    public long append(String topic, String key, String value) throws IOException {
        validateTopic(topic);

        var log = getOrCreateTopic(topic);

        return log.append(key, value);
    }

    @Override
    public List<BrokerRecord> fetch(String topic, long fromOffset, int maxRecords) throws IOException {
        validateTopic(topic);

        var log = getExistingTopic(topic);

        if (log == null) {
            return List.of();
        }

        return log.fetch(fromOffset, maxRecords);
    }

    private FileTopicLog getOrCreateTopic(String topic) throws IOException {
        var existing = topics.get(topic);

        if (existing != null) {
            return existing;
        }

        topicRegistryLock.lock();

        try {
            existing = topics.get(topic);

            if (existing != null) {
                return existing;
            }

            var created = new FileTopicLog(topicDirectory(topic));
            topics.put(topic, created);

            return created;

        } finally {
            topicRegistryLock.unlock();
        }
    }

    private FileTopicLog getExistingTopic(String topic) throws IOException {
        var existing = topics.get(topic);

        if (existing != null) {
            return existing;
        }

        var logFile = topicDirectory(topic).resolve(FileTopicLog.LOG_FILE_NAME);

        if (!Files.exists(logFile)) {
            return null;
        }

        return getOrCreateTopic(topic);
    }

    private Path topicDirectory(String topic) {
        return dataDirectory.resolve(topic);
    }

    private void validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be blank");
        }

        if (!TOPIC_PATTERN.matcher(topic).matches() || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("Invalid topic name: " + topic);
        }
    }

    @Override
    public void close() throws IOException {
        topicRegistryLock.lock();

        try {
            var failures = new ArrayList<IOException>();

            for (var log : topics.values()) {
                try {
                    log.close();
                } catch (IOException exception) {
                    failures.add(exception);
                }
            }

            topics.clear();

            if (!failures.isEmpty()) {
                var exception = new IOException("Failed to close one or more topic logs");
                failures.forEach(exception::addSuppressed);
                throw exception;
            }

        } finally {
            topicRegistryLock.unlock();
        }
    }
}