package dev.odyssey.minikafka.broker;

import dev.odyssey.minikafka.common.BrokerRecord;
import dev.odyssey.minikafka.protocol.BrokerProtocol;
import dev.odyssey.minikafka.storage.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final ServerSocket serverSocket;
    private final MessageStore messageStore;

    private final ExecutorService clientExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean running;

    private Thread acceptThread;

    public BrokerServer(int port, MessageStore messageStore) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.messageStore = messageStore;
    }

    public void start() {
        if (running) {
            throw new IllegalStateException(
                    "Broker already started"
            );
        }

        running = true;

        acceptThread = Thread.ofPlatform()
                .name("broker-acceptor")
                .start(this::acceptLoop);

        log.info("Mini Kafka broker started on port {}", getPort());
    }

    private void acceptLoop() {
        while (running) {
            try {
                var socket = serverSocket.accept();
                clientExecutor.submit(() -> handleClient(socket));
            } catch (SocketException exception) {
                if (running) {
                    log.error("Socket error while accepting connection", exception);
                }
            } catch (IOException exception) {
                if (running) {
                    log.error("Failed to accept client connection", exception);
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        log.debug( "Client connected: {}", socket.getRemoteSocketAddress());
        try (socket;
             var input = new DataInputStream(socket.getInputStream());
             var output = new DataOutputStream(socket.getOutputStream())) {

            while (running && !socket.isClosed()) {
                byte requestType;
                try {
                    requestType = input.readByte();
                } catch (EOFException exception) {
                    break;
                }

                try {
                    handleRequest(requestType, input, output);
                } catch (Exception exception) {
                    log.warn("Request processing failed", exception);

                    output.writeByte(BrokerProtocol.STATUS_ERROR);

                    output.writeUTF(
                            exception.getMessage() == null
                                    ? "Unknown broker error"
                                    : exception.getMessage()
                    );

                    output.flush();
                }
            }

        } catch (IOException exception) {
            log.debug("Client connection closed: {}", exception.getMessage());
        }
    }

    private void handleRequest(byte requestType, DataInputStream input, DataOutputStream output) throws IOException {
        switch (requestType) {
            case BrokerProtocol.REQUEST_PING ->
                    handlePing(output);
            case BrokerProtocol.REQUEST_PRODUCE ->
                    handleProduce(input, output);
            case BrokerProtocol.REQUEST_FETCH ->
                    handleFetch(input, output);
            default ->
                    throw new IllegalArgumentException(
                            "Unknown request type: " + requestType
                    );
        }
    }

    private void handlePing(DataOutputStream output) throws IOException {
        output.writeByte(BrokerProtocol.STATUS_OK);
        output.writeUTF("PONG");
        output.flush();
    }

    private void handleProduce(DataInputStream input,DataOutputStream output) throws IOException {
        var topic = input.readUTF();
        var key = input.readUTF();
        var value = input.readUTF();

        var offset = messageStore.append(topic, key, value);

        output.writeByte(BrokerProtocol.STATUS_OK);
        output.writeLong(offset);

        output.flush();

        log.debug("Produced record topic={} offset={}", topic, offset);
    }

    private void handleFetch(DataInputStream input, DataOutputStream output) throws IOException {
        var topic = input.readUTF();
        var fromOffset = input.readLong();
        var maxRecords = input.readInt();

        var records = messageStore.fetch(topic, fromOffset, maxRecords);

        output.writeByte(BrokerProtocol.STATUS_OK);

        output.writeInt(records.size());

        for (var record : records) {
            output.writeLong(record.offset());
            output.writeLong(record.timestamp());
            output.writeUTF(record.key());
            output.writeUTF(record.value());
        }

        output.flush();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void awaitTermination() throws InterruptedException {
        var thread = acceptThread;
        if (thread != null) {
            thread.join();
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException exception) {
            log.warn(
                    "Failed to close server socket",
                    exception
            );
        }

        clientExecutor.shutdownNow();

        log.info("Mini Kafka broker stopped");
    }
}