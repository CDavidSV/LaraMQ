package server;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientSession {
    private static final Runnable NO_OP = () -> {
    };

    private final UUID clientId;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final Map<String, ConcurrentLinkedQueue<byte[]>> undeliveredMessages = new ConcurrentHashMap<>();
    private final Runnable dirtyCallback;
    private ClientConnection clientConnection;
    private final Object stateLock = new Object();

    public ClientSession(UUID clientId, Runnable dirtyCallback) {
        this.clientId = clientId;
        this.dirtyCallback = dirtyCallback == null ? NO_OP : dirtyCallback;
    }

    public static ClientSession fromData(ClientSessionData data, Runnable dirtyCallback) {
        ClientSession session = new ClientSession(data.clientId(), dirtyCallback);

        Collections.addAll(session.subscribedTopics, data.subscribedTopics());

        data.undeliveredMessages().forEach((topic, messages) -> {
            ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<>();
            for (byte[] message : messages) {
                queue.add(message.clone());
            }
            session.undeliveredMessages.put(topic, queue);
        });

        return session;
    }

    public void subscribeTopic(String topic) {
        synchronized (stateLock) {
            if (subscribedTopics.add(topic)) {
                markDirty();
            }
        }
    }

    public void unsubscribeTopic(String topic) {
        synchronized (stateLock) {
            if (subscribedTopics.remove(topic)) {
                markDirty();
            }
        }
    }

    public void enqueueUndeliveredMessage(String topic, byte[] message) {
        synchronized (stateLock) {
            undeliveredMessages.computeIfAbsent(topic, ignored -> new ConcurrentLinkedQueue<>()).add(message.clone());
            markDirty();
        }
    }

    public void clearSessionData() {
        synchronized (stateLock) {
            boolean hadData = !subscribedTopics.isEmpty() || !undeliveredMessages.isEmpty();
            subscribedTopics.clear();
            undeliveredMessages.clear();
            if (hadData) {
                markDirty();
            }
        }
    }

    public ClientSessionData toData() {
        Map<String, byte[][]> persistedMessages = new HashMap<>();

        synchronized (stateLock) {
            undeliveredMessages.forEach((topic, messages) -> persistedMessages.put(
                    topic,
                    messages.stream()
                            .map(byte[]::clone)
                            .toArray(byte[][]::new)
            ));

            return new ClientSessionData(
                    clientId,
                    subscribedTopics.toArray(new String[0]),
                    persistedMessages
            );
        }
    }

    public ClientConnection getClientConnection() {
        synchronized (stateLock) {
            return clientConnection;
        }
    }

    public void setClientConnection(ClientConnection clientConnection) {
        synchronized (stateLock) {
            this.clientConnection = clientConnection;
        }
    }

    public void flushUndeliveredMessages() throws IOException {
        boolean updated = false;

        synchronized (stateLock) {
            for (Map.Entry<String, ConcurrentLinkedQueue<byte[]>> entry : undeliveredMessages.entrySet()) {
                String topic = entry.getKey();
                ConcurrentLinkedQueue<byte[]> messages = entry.getValue();
                while (!messages.isEmpty()) {
                    ClientConnection conn = clientConnection;
                    if (conn == null) {
                        break;
                    }

                    byte[] message = messages.peek();
                    if (message == null) {
                        break;
                    }

                    conn.sendMessage(topic, message);
                    messages.poll();
                    updated = true;
                }

                if (messages.isEmpty()) {
                    undeliveredMessages.remove(topic, messages);
                    updated = true;
                }
            }
        }

        if (updated) {
            markDirty();
        }
    }

    private void markDirty() {
        dirtyCallback.run();
    }
}