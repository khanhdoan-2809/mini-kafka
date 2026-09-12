package dev.odyssey.minikafka.client;

import dev.odyssey.minikafka.common.BrokerRecord;

import java.util.List;

public class ConsumerMain {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: ConsumerMain <topic> <offset>");
            System.exit(1);
        }

        var topic = args[0];
        var offset = Long.parseLong(args[1]);

        try (var client = new MiniKafkaClient("localhost", 9092)) {
            var records = client.fetch(topic, offset, 100);
            for (var record : records) {
                System.out.printf(
                        "offset=%d key=%s value=%s%n",
                        record.offset(),
                        record.key(),
                        record.value()
                );
            }
        }
    }
}