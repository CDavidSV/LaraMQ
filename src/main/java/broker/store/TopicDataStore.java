package broker.store;

public abstract class TopicDataStore {
    public abstract void save(TopicData topicData);

    public abstract TopicData[] getAll();

    public abstract TopicData get(String topicName);

    public abstract void delete(String topicName);

    public abstract void shutdown();
}
