package server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ClientSessionTest {

    @Test
    void subscribeAndUnsubscribeTopicUpdateSessionState() {
        AtomicInteger dirtyCounter = new AtomicInteger();
        ClientSession session = new ClientSession(UUID.randomUUID(), dirtyCounter::incrementAndGet);

        session.subscribeTopic("weather");
        session.unsubscribeTopic("weather");

        ClientSessionData data = session.toData();
        assertEquals(0, data.subscribedTopics().length);
        assertEquals(2, dirtyCounter.get());
    }

    @Test
    void enqueueUndeliveredMessageStoresDefensiveCopy() {
        ClientSession session = new ClientSession(UUID.randomUUID(), null);
        byte[] payload = new byte[]{1, 2, 3};

        session.enqueueUndeliveredMessage("weather", payload);
        payload[0] = 9;

        byte[] persisted = session.toData().undeliveredMessages().get("weather")[0];
        assertArrayEquals(new byte[]{1, 2, 3}, persisted);
    }

    @Test
    void flushUndeliveredMessagesSendsAndClearsQueue() throws IOException {
        ClientSession session = new ClientSession(UUID.randomUUID(), null);
        ClientConnection connection = mock(ClientConnection.class);

        session.enqueueUndeliveredMessage("weather", "sunny".getBytes());
        session.enqueueUndeliveredMessage("weather", "rainy".getBytes());
        session.setClientConnection(connection);

        session.flushUndeliveredMessages();

        verify(connection, times(2)).sendMessage(org.mockito.ArgumentMatchers.eq("weather"), org.mockito.ArgumentMatchers.any(byte[].class));
        assertFalse(session.toData().undeliveredMessages().containsKey("weather"));
    }

    @Test
    void flushUndeliveredMessagesWithoutConnectionKeepsQueue() throws IOException {
        ClientSession session = new ClientSession(UUID.randomUUID(), null);
        session.enqueueUndeliveredMessage("weather", "offline".getBytes());

        session.flushUndeliveredMessages();

        Map<String, byte[][]> undelivered = session.toData().undeliveredMessages();
        assertTrue(undelivered.containsKey("weather"));
        assertEquals(1, undelivered.get("weather").length);
    }

    @Test
    void clearSessionDataRemovesTopicsAndQueuedMessages() {
        ClientSession session = new ClientSession(UUID.randomUUID(), null);
        session.subscribeTopic("weather");
        session.enqueueUndeliveredMessage("weather", "queued".getBytes());

        session.clearSessionData();

        ClientSessionData data = session.toData();
        assertEquals(0, data.subscribedTopics().length);
        assertEquals(0, data.undeliveredMessages().size());
    }
}

