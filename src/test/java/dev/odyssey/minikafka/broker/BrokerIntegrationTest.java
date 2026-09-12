package dev.odyssey.minikafka.broker;

import dev.odyssey.minikafka.client.MiniKafkaClient;
import dev.odyssey.minikafka.common.BrokerRecord;
import dev.odyssey.minikafka.storage.InMemoryMessageStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerIntegrationTest {

    private BrokerServer broker;

    @BeforeEach
    void setUp() throws Exception {
        broker = new BrokerServer(0, new InMemoryMessageStore());
        broker.start();
    }

    @AfterEach
    void tearDown() {
        broker.close();
    }

    @Test
    void shouldRespondToPing() throws Exception {
        try (var client = new MiniKafkaClient("localhost", broker.getPort())) {
            assertThat(client.ping())
                    .isEqualTo("PONG");
        }
    }

    @Test
    void shouldProduceAndFetchRecords() throws Exception {
        try (var client = new MiniKafkaClient("localhost", broker.getPort())) {
            var offset0 = client.produce("orders", "order-1", "CREATED");
            long offset1 = client.produce("orders", "order-2", "PAID");

            assertThat(offset0)
                    .isEqualTo(0);

            assertThat(offset1)
                    .isEqualTo(1);

            var records = client.fetch("orders", 0, 100);

            assertThat(records)
                    .hasSize(2);

            assertThat(records.get(0).key())
                    .isEqualTo("order-1");

            assertThat(records.get(0).value())
                    .isEqualTo("CREATED");

            assertThat(records.get(1).key())
                    .isEqualTo("order-2");

            assertThat(records.get(1).value())
                    .isEqualTo("PAID");
        }
    }

    @Test
    void shouldFetchFromSpecificOffset() throws Exception {
        try (var client = new MiniKafkaClient("localhost", broker.getPort())) {
            client.produce("orders", "1", "A");
            client.produce("orders", "2", "B");
            client.produce("orders", "3", "C");

            var records = client.fetch("orders", 1, 100);

            assertThat(records)
                    .extracting(BrokerRecord::offset)
                    .containsExactly(1L, 2L);
        }
    }
}