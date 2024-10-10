package Utils;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class Broker implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String host;
    private final int port;

    public Broker(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" +
                " brokerName='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
