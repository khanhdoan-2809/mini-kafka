package dev.odyssey.minikafka.common;

public record BrokerRecord(
        long offset,
        long timestamp,
        String key,
        String value
) {
}