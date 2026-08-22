package broker.store;

public record TopicData(String topicName, byte[] retainedMessage) {
}
