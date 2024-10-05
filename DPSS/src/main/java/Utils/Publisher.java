package Utils;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;
@Getter
public class Publisher {
    private final String name;
    private final String host;
    private final int port;
    private final Set<Topic> topics;

    public Publisher(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.topics = new HashSet<>();
    }

    public void addTopic(Topic topic) {
        topics.add(topic);
    }

    public void removeTopic(Topic topic) {
        topics.remove(topic);
    }

    @Override
    public String toString() {
        return "SubscriberInfo{" +
                "subscriberName='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", topics=" + topics +
                '}';
    }
}
