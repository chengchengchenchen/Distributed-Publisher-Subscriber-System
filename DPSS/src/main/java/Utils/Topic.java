package Utils;

import lombok.Getter;

import java.util.UUID;

@Getter
public class Topic {
    private final String topicId;
    private final String name;

    public Topic(String name) {
        this.topicId = UUID.randomUUID().toString();
        this.name = name;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "topicId='" + topicId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
