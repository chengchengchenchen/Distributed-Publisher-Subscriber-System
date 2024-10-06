package Utils;

import lombok.Getter;

import java.io.Serializable;
import java.util.UUID;

@Getter
public class Topic implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String topicId;
    private final String name;

    public Topic(String topicId, String name) {
        this.topicId = topicId;
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
