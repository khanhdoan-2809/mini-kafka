package dev.odyssey.minikafka.client;

public class ProducerMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ProducerMain <topic> <key> <value>");

            System.exit(1);
        }

        var topic = args[0];
        var key = args[1];
        var value = args[2];

        try (var client = new MiniKafkaClient("localhost", 9092)) {
            var offset = client.produce(topic, key, value);
            System.out.printf("Produced topic=%s offset=%d%n" ,topic ,offset);
        }
    }
}