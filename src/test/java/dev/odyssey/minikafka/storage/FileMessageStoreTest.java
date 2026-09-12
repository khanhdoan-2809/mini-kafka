package dev.odyssey.minikafka.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

class FileMessageStoreTest {

    @TempDir
    Path tempDirectory;

    @Test
    void shouldPersistRecordsAcrossRestart() throws Exception {
        try (var store = new FileMessageStore(tempDirectory)) {
            assertThat(store.append("orders", "order-1", "CREATED")).isEqualTo(0);
            assertThat(store.append("orders", "order-2", "PAID")).isEqualTo(1);
        }

        try (var store = new FileMessageStore(tempDirectory)) {
            var records = store.fetch("orders", 0, 100);

            assertThat(records).hasSize(2);
            assertThat(records.get(0).offset()).isEqualTo(0);
            assertThat(records.get(0).value()).isEqualTo("CREATED");
            assertThat(records.get(1).offset()).isEqualTo(1);
            assertThat(records.get(1).value()).isEqualTo("PAID");
        }
    }

    @Test
    void shouldContinueOffsetAfterRestart() throws Exception {
        try (var store = new FileMessageStore(tempDirectory)) {
            store.append("orders", "order-1", "CREATED");
            store.append("orders", "order-2", "PAID");
        }

        try (var store = new FileMessageStore(tempDirectory)) {
            var offset = store.append("orders", "order-3", "SHIPPED");

            assertThat(offset).isEqualTo(2);
        }
    }

    @Test
    void shouldFetchFromRequestedOffset() throws Exception {
        try (var store = new FileMessageStore(tempDirectory)) {
            store.append("orders", "1", "A");
            store.append("orders", "2", "B");
            store.append("orders", "3", "C");

            var records = store.fetch("orders", 1, 100);

            assertThat(records)
                    .extracting(record -> record.offset())
                    .containsExactly(1L, 2L);
        }
    }

    @Test
    void shouldRecoverFromPartialTail() throws Exception {
        try (var store = new FileMessageStore(tempDirectory)) {
            store.append("orders", "order-1", "CREATED");
        }

        var logFile = tempDirectory
                .resolve("orders")
                .resolve(FileTopicLog.LOG_FILE_NAME);

        Files.write(logFile, new byte[]{0x01, 0x02}, StandardOpenOption.APPEND);

        // restart must remove the partial tail
        try (var store = new FileMessageStore(tempDirectory)) {
            var offset = store.append("orders", "order-2", "PAID");

            assertThat(offset).isEqualTo(1);

            var records = store.fetch("orders", 0, 100);

            assertThat(records)
                    .extracting(record -> record.offset())
                    .containsExactly(0L, 1L);
        }
    }
}