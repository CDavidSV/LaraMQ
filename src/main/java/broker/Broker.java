package broker;

import server.ClientConnection;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Broker {
    private static final Logger logger = Logger.getLogger(Broker.class.getName());
    private final Map<String, Set<ClientConnection>> subscriptions = new ConcurrentHashMap<>();

    public void subscribe(String topic, ClientConnection connection) {
        Set<ClientConnection> clientConnections = subscriptions.computeIfAbsent(topic, _ -> ConcurrentHashMap.newKeySet());
        clientConnections.add(connection);
    }

    public void unsubscribe(String topic, ClientConnection connection) {
        subscriptions.computeIfPresent(topic, (_t, conns) -> {
            conns.remove(connection);
            return conns.isEmpty() ? null : conns;
        });
    }

    public void unsubscribeAll(ClientConnection connection) {
        for (String topic : subscriptions.keySet()) {
            unsubscribe(topic, connection);
        }
    }

    public void publish(String topic, byte[] payload) {
        Set<ClientConnection> clientConnections = subscriptions.get(topic);
        if (clientConnections == null) return;

        for (ClientConnection connection : clientConnections) {
            try {
                connection.sendMessage(topic, payload);
            } catch (IOException e) {
                logger.warning("failed to send notification to client (ID: %s): %s".formatted(connection.getId(), e.getMessage()));
            }
        }
    }

    public String[] listTopics() {
        return subscriptions.keySet().toArray(new String[0]);
    }
}
