package broker;

import broker.store.TopicData;
import broker.store.TopicDataStore;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Broker {
    private static final Logger logger = Logger.getLogger(Broker.class.getName());
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final TopicDataStore topicDataStore;
    private final Object topicLock = new Object();
    private final AnalyticsService analyticsService;
    private final Function<String, ClientConnection> clientConnectionLookup;

    public Broker(TopicDataStore topicDataStore, AnalyticsService analyticsService, Function<String, ClientConnection> clientConnectionLookup) {
        this.topicDataStore = topicDataStore;
        this.analyticsService = analyticsService;
        this.clientConnectionLookup = clientConnectionLookup;

        boolean loadedData = false;
        for (TopicData topicData : topicDataStore.getAll()) {
            loadedData = true;
            Topic topic = new Topic(topicData.topicName());
            topic.setRetainedMessage(topicData.retainedMessage());
            topics.put(topicData.topicName(), topic);

            for (String subscriber : topicData.subscribers()) {
                topic.addSubscriber(subscriber);
            }
        }

        if (loadedData) {
            logger.info("Loaded topic data from persistent storage");
        }
    }

    public Topic subscribe(String topic, String clientId) {
        synchronized (topicLock) {
            Topic t = topics.computeIfAbsent(topic, Topic::new);
            t.addSubscriber(clientId);

            topicDataStore.save(new TopicData(topic, t.getRetainedMessage(), t.getSubscribers().toArray(new String[0])));

            return t;
        }
    }

    public void unsubscribe(String topic, String clientId) {
        synchronized (topicLock) {
            Topic topicObj = topics.get(topic);
            if (topicObj == null) {
                return;
            }

            topicObj.removeSubscriber(clientId);

            topicDataStore.save(new TopicData(topic, topicObj.getRetainedMessage(), topicObj.getSubscribers().toArray(new String[0])));

            if (topicObj.canBeRemoved() && topics.remove(topic, topicObj)) {
                topicDataStore.delete(topic);
            }
        }
    }

    public void unsubscribeAll(String clientId) {
        for (String topic : List.copyOf(topics.keySet())) {
            unsubscribe(topic, clientId);
        }
    }

    public void publish(String topic, byte[] payload, boolean retain) {
        byte[] safePayload = payload == null ? new byte[0] : payload.clone();

        Topic t;

        if (retain && safePayload.length == 0) {
            synchronized (topicLock) {
                t = topics.get(topic);
                if (t == null) return;

                t.clearRetainedMessage();
                if (t.canBeRemoved()) {
                    topics.remove(topic, t);
                }
                topicDataStore.delete(topic);
            }
            return;
        }

        if (retain) {
            synchronized (topicLock) {
                t = topics.computeIfAbsent(topic, Topic::new);
                t.setRetainedMessage(safePayload);
                topicDataStore.save(new TopicData(topic, safePayload, t.getSubscribers().toArray(new String[0])));
            }
        } else {
            t = topics.get(topic);
        }

        if (t == null) return;

        List<String> subscribers = List.copyOf(t.getSubscribers());
        if (subscribers.isEmpty()) return;

        Thread.startVirtualThread(() -> broadcast(topic, safePayload, subscribers));
    }

    private void broadcast(String topic, byte[] payload, List<String> subscribers) {
        for (String clientId : subscribers) {
            ClientConnection connection = clientConnectionLookup.apply(clientId);
            if (connection == null) {
                continue;
            }

            try {
                connection.sendMessage(topic, payload);
                analyticsService.recordNotification();
            } catch (IOException e) {
                logger.warning("failed to send notification to client (ID: %s): %s".formatted(clientId, e.getMessage()));
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
