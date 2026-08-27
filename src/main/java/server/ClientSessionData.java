package server;

import java.util.Map;
import java.util.UUID;

public record ClientSessionData(UUID clientId, String[] subscribedTopics, Map<String, byte[][]> undeliveredMessages) {
    public ClientSessionData {
        subscribedTopics = subscribedTopics == null ? new String[0] : subscribedTopics.clone();
        undeliveredMessages = undeliveredMessages == null ? Map.of() : Map.copyOf(undeliveredMessages);
    }
}

