package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.util.List;

public interface MessageStore {

    long append(String topic, String key, String value);

    List<BrokerRecord> fetch(String topic, long fromOffset, int maxRecords);
}