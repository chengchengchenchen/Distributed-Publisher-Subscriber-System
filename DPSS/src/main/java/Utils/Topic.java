/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Utils;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Getter
public class Topic implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String topicId;
    private final String name;
    private final Publisher publisher;
    public Topic(String topicId, String name, Publisher publisher) {
        this.topicId = topicId;
        this.name = name;
        this.publisher = publisher;
    }
    public Topic(String topicId) {
        this.topicId = topicId;
        this.name = null;
        this.publisher = null;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "topicId='" + topicId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic)) return false;
        Topic topic = (Topic) o;
        return Objects.equals(topicId, topic.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId);
    }
}
