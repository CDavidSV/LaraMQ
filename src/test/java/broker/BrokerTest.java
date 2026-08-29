package broker;

import broker.store.TopicDataStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class BrokerTest {

    @TempDir
    Path tempDir;

    private TopicDataStore topicDataStore;
    private ConcurrentMap<String, ClientConnection> clientConnections;
    private Broker broker;

    @BeforeEach
    void setUp() {
        topicDataStore = new TopicDataStore(tempDir.resolve("topic_data.json"));
        clientConnections = new ConcurrentHashMap<>();
        broker = new Broker(topicDataStore, new AnalyticsService(), clientConnections::get);
    }

    @AfterEach
    void tearDown() {
        broker.shutdown();
    }

    @Test
    void subscribeAddsSubscriber() {
        UUID clientId = UUID.randomUUID();

        Topic topic = broker.subscribe("weather", clientId.toString());

        assertTrue(topic.getSubscribers().contains(clientId.toString()));
    }

    @Test
    void unsubscribeRemovesSubscriberAndDeletesEmptyTopic() {
        UUID clientId = UUID.randomUUID();
        broker.subscribe("weather", clientId.toString());

        broker.unsubscribe("weather", clientId.toString());

        assertFalse(Arrays.asList(broker.listTopics()).contains("weather"));
    }

    @Test
    void publishSendsMessageToConnectedSubscribers() throws IOException {
        UUID clientId = UUID.randomUUID();
        broker.subscribe("weather", clientId.toString());

        ClientConnection connection = mock(ClientConnection.class);
        clientConnections.put(clientId.toString(), connection);

        byte[] payload = "sunny".getBytes();
        broker.publish("weather", payload, false);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(connection, timeout(1000)).sendMessage(eq("weather"), payloadCaptor.capture());
        assertArrayEquals(payload, payloadCaptor.getValue());
    }

    @Test
    void publishSkipsOfflineSubscriber() {
        UUID clientId = UUID.randomUUID();
        broker.subscribe("weather", clientId.toString());
        ClientConnection offlineConnection = mock(ClientConnection.class);

        byte[] payload = "rainy".getBytes();
        broker.publish("weather", payload, false);

        verifyNoInteractions(offlineConnection);
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

        broker.subscribe("weather", clientId.toString());
        broker.subscribe("news", clientId.toString());

        broker.unsubscribeAll(clientId.toString());

        assertEquals(0, broker.listTopics().length);
    }
}




