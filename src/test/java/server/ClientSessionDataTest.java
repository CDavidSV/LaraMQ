package server;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientSessionDataTest {

    @Test
    void constructorUsesSafeDefaultsWhenInputsAreNull() {
        ClientSessionData data = new ClientSessionData(UUID.randomUUID(), null, null);

        assertEquals(0, data.subscribedTopics().length);
        assertEquals(0, data.undeliveredMessages().size());
    }

    @Test
    void constructorDefensivelyCopiesSubscribedTopics() {
        String[] topics = new String[]{"weather"};

        ClientSessionData data = new ClientSessionData(UUID.randomUUID(), topics, Map.of());
        topics[0] = "news";

        assertArrayEquals(new String[]{"weather"}, data.subscribedTopics());
    }

    @Test
    void constructorCreatesUnmodifiableUndeliveredMessagesMap() {
        Map<String, byte[][]> messages = new HashMap<>();
        messages.put("weather", new byte[][]{{1}});

        ClientSessionData data = new ClientSessionData(UUID.randomUUID(), new String[0], messages);

        assertThrows(UnsupportedOperationException.class, () -> data.undeliveredMessages().put("news", new byte[][]{{2}}));
    }
}

