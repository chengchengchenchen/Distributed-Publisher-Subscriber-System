/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Utils;

import lombok.Getter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
@Getter
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Topic topic;
    private final String payload;
    private final LocalDateTime timestamp;

    public Message(Topic topic, String payload) {
        this.topic = topic;
        this.payload = payload;
        this.timestamp = LocalDateTime.now();
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", payload='" + payload + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
    public String getFormattedMessage() {
        return String.format("[%s] [%s:%s] [%s]",
                timestamp.format(DateTimeFormatter.ofPattern("dd/MM HH:mm:ss")),
                topic.getTopicId(),
                topic.getName(),
                payload);
    }

}
