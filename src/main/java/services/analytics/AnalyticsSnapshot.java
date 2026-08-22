package services.analytics;

import java.util.Map;

public record AnalyticsSnapshot(
        long startedAtMillis,
        long totalPublishes,
        long totalSubscribes,
        long totalUnsubscribes,
        long totalNotifications,
        long totalCommands,
        long connectedClients,
        Map<String, Long> publishCountByTopic,
        Map<String, Long> subscriberCountByTopic,
        Map<String, Long> commandCountByType
) {
    private String formatMap(Map<String, Long> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            sb.append(String.format("    - %s: %d%n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        String publishCountByTopicStr = formatMap(publishCountByTopic);
        String subscriberCountByTopicStr = formatMap(subscriberCountByTopic);
        String commandCountByTypeStr = formatMap(commandCountByType);

        return """
                Analytics:
                - Started At: %d
                - Total publishes: %d
                - Total subscribes: %d
                - Total unsubscribes: %d
                - Total notifications send: %d
                - Total commands used: %d
                - Connected clients: %d
                - Publish Count By Topic:
                %s
                - Subscriber Count By Topic:
                %s
                - Command Count By Type:
                %s
                """.formatted(
                startedAtMillis,
                totalPublishes,
                totalSubscribes,
                totalUnsubscribes,
                totalNotifications,
                totalCommands,
                connectedClients,
                publishCountByTopicStr,
                subscriberCountByTopicStr,
                commandCountByTypeStr
        );

    }
}
