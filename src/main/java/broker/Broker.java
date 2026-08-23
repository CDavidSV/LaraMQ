package broker;

import broker.store.TopicData;
import broker.store.TopicDataStore;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Broker {
    private static final Logger logger = Logger.getLogger(Broker.class.getName());
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final TopicDataStore topicDataStore;
    private final Object topicLock = new Object();
    private final AnalyticsService analyticsService;

    public Broker(TopicDataStore topicDataStore, AnalyticsService analyticsService) {
        this.topicDataStore = topicDataStore;
        this.analyticsService = analyticsService;

        for (TopicData topicData : topicDataStore.getAll()) {
            Topic topic = new Topic(topicData.topicName());
            topic.setRetainedMessage(topicData.retainedMessage());
            topics.put(topicData.topicName(), topic);
        }
    }

    public Topic subscribe(String topic, ClientConnection connection) {
        synchronized (topicLock) {
            Topic t = topics.computeIfAbsent(topic, Topic::new);
            t.addSubscriber(connection);

            return t;
        }
    }

    public void unsubscribe(String topic, ClientConnection connection) {
        synchronized (topicLock) {
            Topic topicObj = topics.get(topic);
            if (topicObj == null) {
                return;
            }

            topicObj.removeSubscriber(connection);

            if (topicObj.canBeRemoved() && topics.remove(topic, topicObj)) {
                topicDataStore.delete(topic);
            }
        }
    }

    public void unsubscribeAll(ClientConnection connection) {
        for (String topic : topics.keySet()) {
            unsubscribe(topic, connection);
        }
    }

    public void publish(String topic, byte[] payload, boolean retain) {
        byte[] safePayload = payload == null ? new byte[0] : payload.clone();

        Topic t;

        if (retain && safePayload.length == 0) {
            synchronized (topicLock) {
                t = topics.get(topic);
                if (t == null) return;

                synchronized (t) {
                    t.clearRetainedMessage();
                    topicDataStore.delete(topic);
                    if (t.canBeRemoved()) {
                        topics.remove(topic, t);
                    }
                }
            }
            return;
        }

        if (retain) {
            synchronized (topicLock) {
                t = topics.computeIfAbsent(topic, Topic::new);
                synchronized (t) {
                    t.setRetainedMessage(safePayload);
                    topicDataStore.save(new TopicData(topic, safePayload));
                }
            }
        } else {
            t = topics.get(topic);
        }

        if (t == null) return;

        List<ClientConnection> subscribers = List.copyOf(t.getSubscribers());
        if (subscribers.isEmpty()) return;

        Thread.startVirtualThread(() -> broadcast(topic, safePayload, subscribers));
    }

    private void broadcast(String topic, byte[] payload, List<ClientConnection> subscribers) {
        for (ClientConnection connection : subscribers) {
            try {
                connection.sendMessage(topic, payload);
                analyticsService.recordNotification();
            } catch (IOException e) {
                logger.warning("failed to send notification to client (ID: %s): %s".formatted(connection.getId(), e.getMessage()));
            }
        }
    }

    public String[] listTopics() {
        return topics.keySet().toArray(new String[0]);
    }

    public void shutdown() {
        topicDataStore.shutdown();
    }
}
