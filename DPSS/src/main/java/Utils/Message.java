package Utils;

import lombok.Getter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
@Getter
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Topic topic;
    private final String messageId;
    private final String payload;
    private final List<String> processedBrokers;
    private final LocalDateTime timestamp;

    public Message(Topic topic, String payload, int ttl) {
        this.topic = topic;
        this.messageId = UUID.randomUUID().toString();  // 生成唯一消息ID
        this.payload = payload;
        this.processedBrokers = new ArrayList<>();
        this.timestamp = LocalDateTime.now();  // 发布时的时间戳
    }

    public void addProcessedBroker(String brokerId) {
        this.processedBrokers.add(brokerId);
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", payload='" + payload + '\'' +
                ", processedBrokers=" + processedBrokers +
                ", timestamp=" + timestamp +
                '}';
    }
}
