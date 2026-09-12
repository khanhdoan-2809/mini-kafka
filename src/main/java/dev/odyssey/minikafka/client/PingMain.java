package dev.odyssey.minikafka.client;

public class PingMain {

    public static void main(String[] args) throws Exception {
        try (var client = new MiniKafkaClient("localhost", 9092)) {
            System.out.println(client.ping());
        }
    }
}