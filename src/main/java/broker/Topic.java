package broker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String name;
    private byte[] retainedMessage;
    private final Set<String> subscribers = ConcurrentHashMap.newKeySet();

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

    protected Set<String> getSubscribers() {
        return subscribers;
    }

    protected void addSubscriber(String clientId) {
        subscribers.add(clientId);
    }

    protected void removeSubscriber(String clientId) {
        subscribers.remove(clientId);
    }
}
