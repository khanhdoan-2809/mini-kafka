package dev.odyssey.minikafka.storage;

import dev.odyssey.minikafka.common.BrokerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryMessageStoreTest {
    private final InMemoryMessageStore store = new InMemoryMessageStore();

    @Test
    void shouldAssignSequentialOffsets() {
        var firstOffset = store.append("orders", "order-1", "CREATED");
        var secondOffset = store.append("orders", "order-2", "CREATED");

        assertThat(firstOffset)
                .isEqualTo(0);

        assertThat(secondOffset)
                .isEqualTo(1);
    }

    @Test
    void shouldFetchFromRequestedOffset() {
        store.append("orders", "order-1","CREATED");
        store.append("orders","order-2","CREATED");
        store.append("orders","order-3","CREATED");

        List<BrokerRecord> records = store.fetch("orders",1,100
                );

        assertThat(records)
                .hasSize(2);

        assertThat(records.get(0).offset())
                .isEqualTo(1);

        assertThat(records.get(1).offset())
                .isEqualTo(2);
    }

    @Test
    void shouldRespectFetchLimit() {
        store.append("orders","1","A");
        store.append("orders","2","B");
        store.append("orders","3","C");

        var records = store.fetch("orders",0,2);

        assertThat(records)
                .hasSize(2);
    }

    @Test
    void shouldReturnEmptyListForUnknownTopic() {
        var records = store.fetch("unknown",0,100);

        assertThat(records)
                .isEmpty();
    }
}