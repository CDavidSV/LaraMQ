package command;

import broker.Broker;
import broker.Topic;
import org.junit.jupiter.api.Test;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SubscribeCommandTest {

    @Test
    void executeSubscribesClientAndReturnsRetainedMessage() throws Exception {
        Broker broker = mock(Broker.class);
        AnalyticsService analyticsService = mock(AnalyticsService.class);
        SubscribeCommand command = new SubscribeCommand(broker, analyticsService);

        ClientConnection connection = mock(ClientConnection.class);
        UUID clientId = UUID.randomUUID();
        when(connection.getId()).thenReturn(clientId);

        Topic topic = mock(Topic.class);
        byte[] retained = "retained".getBytes(StandardCharsets.UTF_8);
        when(topic.getRetainedMessage()).thenReturn(retained);
        when(broker.subscribe("weather", clientId.toString())).thenReturn(topic);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream("weather".getBytes(StandardCharsets.UTF_8)));
        byte[] response = command.execute(connection, in);

        verify(broker).subscribe(eq("weather"), eq(clientId.toString()));
        verify(analyticsService).recordSubscribe("weather");
        assertArrayEquals(retained, response);
    }

    @Test
    void executeReturnsEmptyArrayWhenNoRetainedMessageExists() throws Exception {
        Broker broker = mock(Broker.class);
        AnalyticsService analyticsService = mock(AnalyticsService.class);
        SubscribeCommand command = new SubscribeCommand(broker, analyticsService);

        ClientConnection connection = mock(ClientConnection.class);
        UUID clientId = UUID.randomUUID();
        when(connection.getId()).thenReturn(clientId);

        Topic topic = mock(Topic.class);
        when(topic.getRetainedMessage()).thenReturn(null);
        when(broker.subscribe("weather", clientId.toString())).thenReturn(topic);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream("weather".getBytes(StandardCharsets.UTF_8)));
        byte[] response = command.execute(connection, in);

        assertArrayEquals(new byte[0], response);
    }
}

