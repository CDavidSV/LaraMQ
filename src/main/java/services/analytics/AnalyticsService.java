package services.analytics;

import command.CommandCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class AnalyticsService {
    private final AtomicLong startedAtMillis = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalPublishes = new LongAdder();
    private final LongAdder totalSubscribes = new LongAdder();
    private final LongAdder totalUnsubscribes = new LongAdder();
    private final LongAdder totalNotifications = new LongAdder();
    private final LongAdder totalCommands = new LongAdder();
    private final LongAdder connectedClients = new LongAdder();

    private final Map<String, LongAdder> publishCountByTopic = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> subscriberCountByTopic = new ConcurrentHashMap<>();
    private final Map<CommandCode, LongAdder> commandCountByType = new ConcurrentHashMap<>();


    public void recordClientConnected() {
        connectedClients.increment();
    }

    public void recordClientDisconnected() {
        connectedClients.decrement();
    }

    public void recordPublish(String topic) {
        totalPublishes.increment();
        publishCountByTopic.computeIfAbsent(topic, k -> new LongAdder()).increment();
    }

    public void recordSubscribe(String topic) {
        totalSubscribes.increment();
        subscriberCountByTopic.computeIfAbsent(topic, k -> new LongAdder()).increment();
    }

    public void recordUnsubscribe(String topic) {
        totalUnsubscribes.increment();
        subscriberCountByTopic.computeIfAbsent(topic, k -> new LongAdder()).decrement();
    }

    public void recordNotification() {
        totalNotifications.increment();
    }

    public void recordCommand(CommandCode commandType) {
        totalCommands.increment();
        commandCountByType.computeIfAbsent(commandType, k -> new LongAdder()).increment();
    }

    public void reset() {
        startedAtMillis.set(System.currentTimeMillis());
        totalPublishes.reset();
        totalSubscribes.reset();
        totalUnsubscribes.reset();
        totalNotifications.reset();
        totalCommands.reset();
        connectedClients.reset();
        publishCountByTopic.clear();
        subscriberCountByTopic.clear();
        commandCountByType.clear();
    }

    public AnalyticsSnapshot getSnapshot() {
        return new AnalyticsSnapshot(
                startedAtMillis.get(),
                totalPublishes.sum(),
                totalSubscribes.sum(),
                totalUnsubscribes.sum(),
                totalNotifications.sum(),
                totalCommands.sum(),
                connectedClients.sum(),
                publishCountByTopic.entrySet().stream().collect(
                        java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum())
                ),
                subscriberCountByTopic.entrySet().stream().collect(
                        java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().sum())
                ),
                commandCountByType.entrySet().stream().collect(
                        java.util.stream.Collectors.toMap(e -> e.getKey().name(), e -> e.getValue().sum())
                )
        );
    }
}
