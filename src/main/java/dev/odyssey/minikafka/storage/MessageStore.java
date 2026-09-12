package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.io.IOException;
import java.util.List;

public interface MessageStore extends AutoCloseable {

    long append(String topic, String key, String value) throws IOException;

    List<BrokerRecord> fetch(String topic, long fromOffset, int maxRecords) throws IOException;

    @Override
    default void close() throws IOException {

    }
}