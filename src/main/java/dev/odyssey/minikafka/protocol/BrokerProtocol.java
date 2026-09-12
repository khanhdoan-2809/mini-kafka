package dev.odyssey.minikafka.protocol;

public final class BrokerProtocol {

    private BrokerProtocol() {
    }

    public static final byte REQUEST_PING = 1;
    public static final byte REQUEST_PRODUCE = 2;
    public static final byte REQUEST_FETCH = 3;

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_ERROR = 1;
}