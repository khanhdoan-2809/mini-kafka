package dev.odyssey.minikafka.client;

import dev.odyssey.minikafka.common.BrokerRecord;
import dev.odyssey.minikafka.protocol.BrokerProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MiniKafkaClient implements AutoCloseable {

    private final Socket socket;
    private final DataInputStream input;
    private final DataOutputStream output;

    public MiniKafkaClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.input = new DataInputStream(socket.getInputStream());
        this.output = new DataOutputStream(socket.getOutputStream());
    }

    public String ping() throws IOException {
        output.writeByte(BrokerProtocol.REQUEST_PING);
        output.flush();

        ensureSuccess();

        return input.readUTF();
    }

    public long produce(String topic, String key, String value) throws IOException {
        output.writeByte(BrokerProtocol.REQUEST_PRODUCE);

        output.writeUTF(topic);
        output.writeUTF(key);
        output.writeUTF(value);

        output.flush();

        ensureSuccess();

        return input.readLong();
    }

    public List<BrokerRecord> fetch(String topic, long fromOffset, int maxRecords) throws IOException {
        output.writeByte(BrokerProtocol.REQUEST_FETCH);

        output.writeUTF(topic);
        output.writeLong(fromOffset);
        output.writeInt(maxRecords);

        output.flush();

        ensureSuccess();

        int recordCount = input.readInt();

        var records = new ArrayList<BrokerRecord>(recordCount);

        for (var i = 0; i < recordCount; i++) {
            var offset = input.readLong();
            var timestamp = input.readLong();
            var key = input.readUTF();
            var value = input.readUTF();

            records.add(new BrokerRecord(offset, timestamp, key, value));
        }

        return records;
    }

    private void ensureSuccess() throws IOException {
        var status = input.readByte();
        if (status == BrokerProtocol.STATUS_OK) {
            return;
        }

        var errorMessage = input.readUTF();

        throw new IOException("Broker error: " + errorMessage);
    }

    @Override
    public void close() throws IOException {
        input.close();
        output.close();
        socket.close();
    }
}