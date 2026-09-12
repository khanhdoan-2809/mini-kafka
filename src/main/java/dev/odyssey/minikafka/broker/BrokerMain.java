package dev.odyssey.minikafka.broker;

import dev.odyssey.minikafka.storage.FileMessageStore;

import java.nio.file.Path;

public class BrokerMain {

    private static final int DEFAULT_PORT = 9092;
    private static final Path DEFAULT_DATA_DIRECTORY = Path.of("data");

    public static void main(String[] args) throws Exception {
        var port = args.length >= 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        var dataDirectory = args.length >= 2 ? Path.of(args[1]) : DEFAULT_DATA_DIRECTORY;

        var messageStore = new FileMessageStore(dataDirectory);
        var broker = new BrokerServer(port, messageStore);

        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "broker-shutdown"));

        broker.start();
        broker.awaitTermination();
    }
}