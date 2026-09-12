package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class FileTopicLog implements AutoCloseable {

    static final String LOG_FILE_NAME = "00000000000000000000.log";

    private static final Logger log = LoggerFactory.getLogger(FileTopicLog.class);

    private final FileChannel channel;
    private final RecordCodec codec = new RecordCodec();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private long nextOffset;

    public FileTopicLog(Path topicDirectory) throws IOException {
        Files.createDirectories(topicDirectory);

        var logFile = topicDirectory.resolve(LOG_FILE_NAME);

        channel = FileChannel.open(
                logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        nextOffset = recoverNextOffset();
    }

    public long append(String key, String value) throws IOException {
        writeLock.lock();

        try {
            var offset = nextOffset;
            var record = new BrokerRecord(offset, System.currentTimeMillis(), key, value);
            var frame = codec.encode(record);
            var appendPosition = channel.size();

            channel.position(appendPosition);

            try {
                writeFully(frame);

                /*
                 * Phase 2 durability policy:
                 *
                 * PRODUCE is considered successful only after the OS is asked
                 * to flush file contents to durable storage.
                 *
                 * This is deliberately expensive.
                 * Later batching/ACK phases will improve it.
                 */
                channel.force(false);

                nextOffset++;

                return offset;

            } catch (IOException exception) {
                rollbackPartialAppend(appendPosition, exception);
                throw exception;
            }

        } finally {
            writeLock.unlock();
        }
    }

    public List<BrokerRecord> fetch(long fromOffset, int maxRecords) throws IOException {
        if (fromOffset < 0) {
            throw new IllegalArgumentException("Offset must be >= 0");
        }

        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be > 0");
        }

        readLock.lock();

        try {
            var result = new ArrayList<BrokerRecord>();
            var fileSize = channel.size();
            var bytePosition = 0L;

            while (bytePosition < fileSize && result.size() < maxRecords) {
                // get how many bytes the next record takes up
                var header = ByteBuffer.allocate(Integer.BYTES);
                readFullyAt(header, bytePosition);
                var payloadSize = header.getInt();
                validatePayloadSize(payloadSize);

                // read the record payload
                var payload = ByteBuffer.allocate(payloadSize);
                readFullyAt(payload, bytePosition + Integer.BYTES);

                var record = codec.decode(payload);

                if (record.offset() >= fromOffset) {
                    result.add(record);
                }

                bytePosition += Integer.BYTES + payloadSize;
            }

            return List.copyOf(result);

        } finally {
            readLock.unlock();
        }
    }

    private long recoverNextOffset() throws IOException {
        var fileSize = channel.size();
        var position = 0L;
        var expectedOffset = 0L;

        while (position < fileSize) {
            var remaining = fileSize - position;

            if (remaining < Integer.BYTES) {
                truncatePartialTail(position);
                break;
            }

            // get how many bytes the next record takes up
            var header = ByteBuffer.allocate(Integer.BYTES);
            readFullyAt(header, position);
            var payloadSize = header.getInt();
            validatePayloadSize(payloadSize);

            var frameEnd = position + Integer.BYTES + payloadSize;

            if (frameEnd > fileSize) {
                truncatePartialTail(position);
                break;
            }

            var payload = ByteBuffer.allocate(payloadSize);
            readFullyAt(payload, position + Integer.BYTES);

            var record = codec.decode(payload);

            if (record.offset() != expectedOffset) {
                throw new IOException(
                        "Corrupt log: expected offset %d but found %d at byte %d"
                                .formatted(expectedOffset, record.offset(), position)
                );
            }

            expectedOffset++;
            position = frameEnd;
        }

        channel.position(channel.size()); // put cursor at end of file to append log

        log.info("Recovered log with nextOffset={}", expectedOffset);

        return expectedOffset;
    }

    private void truncatePartialTail(long position) throws IOException {
        log.warn("Truncating incomplete log tail at byte {}", position);

        channel.truncate(position);
        channel.force(false);
    }

    private void rollbackPartialAppend(long appendPosition, IOException originalException) {
        try {
            channel.truncate(appendPosition);
            channel.force(false);
        } catch (IOException rollbackException) {
        //    originalException.addSuppressed(rollbackException);
        }
    }

    private void validatePayloadSize(int payloadSize) throws IOException {
        if (payloadSize < RecordCodec.MIN_PAYLOAD_BYTES) {
            throw new IOException("Invalid record payload size: " + payloadSize);
        }

        if (payloadSize > RecordCodec.MAX_PAYLOAD_BYTES) {
            throw new IOException("Record payload exceeds maximum: " + payloadSize);
        }
    }

    private void writeFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private void readFullyAt(ByteBuffer buffer, long position) throws IOException {
        var currentPosition = position;

        while (buffer.hasRemaining()) {
            var bytesRead = channel.read(buffer, currentPosition);

            if (bytesRead < 0) {
                throw new EOFException("Unexpected end of log");
            }

            currentPosition += bytesRead;
        }

        buffer.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}