package broker;

import broker.store.TopicDataStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import server.ClientConnection;
import server.ClientSession;
import server.ClientSessionData;
import server.ClientSessionHandler;
import services.analytics.AnalyticsService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class BrokerTest {

    @TempDir
    Path tempDir;

    private TopicDataStore topicDataStore;
    private ClientSessionHandler clientSessionHandler;
    private Broker broker;

    private static boolean hasQueuedMessage(ClientSession session, String topic) {
        Map<String, byte[][]> undelivered = session.toData().undeliveredMessages();
        byte[][] messages = undelivered.get(topic);
        return messages != null && messages.length > 0;
    }

    private static void waitForCondition(Check condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.matches()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for async broker work");
            }
        }

        fail("Timed out waiting for async broker work");
    }

    @BeforeEach
    void setUp() {
        topicDataStore = new TopicDataStore(tempDir.resolve("topic_data.json"));
        clientSessionHandler = new ClientSessionHandler(tempDir.resolve("client_sessions.json"));
        broker = new Broker(topicDataStore, new AnalyticsService(), clientSessionHandler);
    }

    @AfterEach
    void tearDown() {
        broker.shutdown();
        clientSessionHandler.close();
    }

    @Test
    void subscribeAddsSubscriberAndUpdatesSessionState() {
        UUID clientId = UUID.randomUUID();
        ClientSession session = clientSessionHandler.getOrCreate(clientId);

        Topic topic = broker.subscribe("weather", clientId.toString());

        assertTrue(topic.getSubscribers().contains(clientId.toString()));
        ClientSessionData persisted = session.toData();
        assertTrue(Arrays.asList(persisted.subscribedTopics()).contains("weather"));
    }

    @Test
    void unsubscribeRemovesSubscriberAndDeletesEmptyTopic() {
        UUID clientId = UUID.randomUUID();
        clientSessionHandler.getOrCreate(clientId);
        broker.subscribe("weather", clientId.toString());

        broker.unsubscribe("weather", clientId.toString());

        assertFalse(Arrays.asList(broker.listTopics()).contains("weather"));
    }

    @Test
    void publishSendsMessageToConnectedSubscribers() throws IOException {
        UUID clientId = UUID.randomUUID();
        ClientSession session = clientSessionHandler.getOrCreate(clientId);
        broker.subscribe("weather", clientId.toString());

        ClientConnection connection = mock(ClientConnection.class);
        session.setClientConnection(connection);

        byte[] payload = "sunny".getBytes();
        broker.publish("weather", payload, false);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(connection, timeout(1000)).sendMessage(eq("weather"), payloadCaptor.capture());
        assertArrayEquals(payload, payloadCaptor.getValue());
    }

    @Test
    void publishEnqueuesUndeliveredMessageForOfflineSubscriber() {
        UUID clientId = UUID.randomUUID();
        ClientSession session = clientSessionHandler.getOrCreate(clientId);
        broker.subscribe("weather", clientId.toString());

        byte[] payload = "rainy".getBytes();
        broker.publish("weather", payload, false);

        waitForCondition(() -> hasQueuedMessage(session, "weather"), 1000);

        ClientSessionData data = session.toData();
        assertTrue(data.undeliveredMessages().containsKey("weather"));
        assertArrayEquals(payload, data.undeliveredMessages().get("weather")[0]);
    }

    @Test
    void retainPublishStoresRetainedMessageForFutureSubscribers() {
        UUID clientId = UUID.randomUUID();
        byte[] retained = "retained".getBytes();

        broker.publish("weather", retained, true);
        Topic topic = broker.subscribe("weather", clientId.toString());

        assertArrayEquals(retained, topic.getRetainedMessage());
    }

    @Test
    void emptyRetainedPublishClearsRetainedMessageAndRemovesTopic() {
        byte[] retained = "retained".getBytes();

        broker.publish("weather", retained, true);
        broker.publish("weather", new byte[0], true);

        assertFalse(Arrays.asList(broker.listTopics()).contains("weather"));
    }

    @Test
    void unsubscribeAllRemovesClientFromEveryTopic() {
        UUID clientId = UUID.randomUUID();
        clientSessionHandler.getOrCreate(clientId);

        broker.subscribe("weather", clientId.toString());
        broker.subscribe("news", clientId.toString());

        broker.unsubscribeAll(clientId.toString());

        assertEquals(0, broker.listTopics().length);
    }

    @FunctionalInterface
    private interface Check {
        boolean matches();
    }
}

