package broker;

import broker.store.TopicData;
import broker.store.TopicDataStore;
import server.ClientConnection;
import server.ClientSession;
import server.ClientSessionHandler;
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
    private final ClientSessionHandler clientSessionHandler;

    public Broker(TopicDataStore topicDataStore, AnalyticsService analyticsService, ClientSessionHandler clientSessionHandler) {
        this.topicDataStore = topicDataStore;
        this.analyticsService = analyticsService;
        this.clientSessionHandler = clientSessionHandler;

        for (TopicData topicData : topicDataStore.getAll()) {
            Topic topic = new Topic(topicData.topicName());
            topic.setRetainedMessage(topicData.retainedMessage());
            topics.put(topicData.topicName(), topic);

            for (String subscriber : topicData.subscribers()) {
                topic.addSubscriber(subscriber);
            }
        }
    }

    public Topic subscribe(String topic, String clientId) {
        synchronized (topicLock) {
            Topic t = topics.computeIfAbsent(topic, Topic::new);
            t.addSubscriber(clientId);

            ClientSession clientSession = clientSessionHandler.get(clientId);
            if (clientSession != null) {
                clientSession.subscribeTopic(topic);
            }

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

            ClientSession clientSession = clientSessionHandler.get(clientId);
            if (clientSession != null) {
                clientSession.unsubscribeTopic(topic);
            }

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
            try {
                ClientSession session = clientSessionHandler.get(clientId);
                if (session != null) {
                    ClientConnection connection = session.getClientConnection();
                    if (connection != null) {
                        connection.sendMessage(topic, payload);
                        analyticsService.recordNotification();
                    } else {
                        session.enqueueUndeliveredMessage(topic, payload);
                    }
                }
            } catch (IOException e) {
                ClientSession session = clientSessionHandler.get(clientId);
                if (session != null) {
                    session.enqueueUndeliveredMessage(topic, payload);
                }
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
