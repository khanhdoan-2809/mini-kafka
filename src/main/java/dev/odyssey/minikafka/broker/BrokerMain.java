package dev.odyssey.minikafka.broker;

import dev.odyssey.minikafka.storage.InMemoryMessageStore;
import dev.odyssey.minikafka.storage.MessageStore;

public class BrokerMain {

    private static final int DEFAULT_PORT = 9092;

    public static void main(String[] args) throws Exception {
        var port = resolvePort(args);

        var messageStore = new InMemoryMessageStore();

        var broker = new BrokerServer(port, messageStore);

        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "broker-shutdown"));
        broker.start();
        broker.awaitTermination();
    }

    private static int resolvePort(String[] args) {
        if (args.length == 0) {
            return DEFAULT_PORT;
        }

        return Integer.parseInt(args[0]);
    }
}