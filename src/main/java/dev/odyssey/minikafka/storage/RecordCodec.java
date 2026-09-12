package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class RecordCodec {

    private static final byte VERSION = 1;
    // 1 + 8 + 8 + 4 + 4 = 25 bytes
    public static final int MIN_PAYLOAD_BYTES =
                    Byte.BYTES      // version
                    + Long.BYTES    // offset
                    + Long.BYTES    // timestamp
                    + Integer.BYTES // keyLength
                    + Integer.BYTES;// valueLength

    public static final int MAX_PAYLOAD_BYTES = 10 * 1024 * 1024;

    ByteBuffer encode(BrokerRecord record) {
        Objects.requireNonNull(record);
        Objects.requireNonNull(record.key());
        Objects.requireNonNull(record.value());

        var key = record.key().getBytes(StandardCharsets.UTF_8);
        var value = record.value().getBytes(StandardCharsets.UTF_8);

        var payloadSize = MIN_PAYLOAD_BYTES + key.length + value.length;

        if (payloadSize > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Record exceeds maximum size: " + payloadSize);
        }

        var frameSize = Integer.BYTES + payloadSize;
        var buffer = ByteBuffer.allocate(frameSize);

        buffer.putInt(payloadSize);
        buffer.put(VERSION);
        buffer.putLong(record.offset());
        buffer.putLong(record.timestamp());

        buffer.putInt(key.length);
        buffer.put(key);

        buffer.putInt(value.length);
        buffer.put(value);

        buffer.flip();

        return buffer;
    }

    BrokerRecord decode(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() < MIN_PAYLOAD_BYTES) {
            throw new IOException("Record payload is too small");
        }

        var version = buffer.get();

        if (version != VERSION) {
            throw new IOException("Unsupported record version: " + version);
        }

        var offset = buffer.getLong();
        var timestamp = buffer.getLong();

        var keyLength = buffer.getInt();
        validateLength("key", keyLength, buffer.remaining());

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        if (buffer.remaining() < Integer.BYTES) {
            throw new IOException("Missing value length");
        }

        var valueLength = buffer.getInt();
        validateLength("value", valueLength, buffer.remaining());

        var valueBytes = new byte[valueLength];
        buffer.get(valueBytes);

        if (buffer.hasRemaining()) {
            throw new IOException("Unexpected bytes at end of record");
        }

        return new BrokerRecord(
                offset,
                timestamp,
                new String(keyBytes, StandardCharsets.UTF_8),
                new String(valueBytes, StandardCharsets.UTF_8)
        );
    }

    private void validateLength(String field, int length, int remaining) throws IOException {
        if (length < 0) {
            throw new IOException(field + " length cannot be negative");
        }

        if (length > remaining) {
            throw new IOException(field + " length exceeds record boundary");
        }
    }
}