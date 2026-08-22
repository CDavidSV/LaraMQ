package broker;

import server.ClientConnection;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String name;
    private byte[] retainedMessage;
    private final Set<ClientConnection> subscribers = ConcurrentHashMap.newKeySet();

    public Topic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public synchronized byte[] getRetainedMessage() {
        return retainedMessage == null ? null : retainedMessage.clone();
    }

    protected synchronized void setRetainedMessage(byte[] retainedMessage) {
        this.retainedMessage = retainedMessage == null ? null : retainedMessage.clone();
    }

    public synchronized void clearRetainedMessage() {
        this.retainedMessage = null;
    }

    protected synchronized boolean canBeRemoved() {
        return subscribers.isEmpty() && retainedMessage == null;
    }

    protected Set<ClientConnection> getSubscribers() {
        return subscribers;
    }

    protected void addSubscriber(ClientConnection connection) {
        subscribers.add(connection);
    }

    protected void removeSubscriber(ClientConnection connection) {
        subscribers.remove(connection);
    }
}
