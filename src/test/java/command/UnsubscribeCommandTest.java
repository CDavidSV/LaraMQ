package command;

import broker.Broker;
import org.junit.jupiter.api.Test;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UnsubscribeCommandTest {

    @Test
    void executeUnsubscribesClientAndRecordsAnalytics() throws Exception {
        Broker broker = mock(Broker.class);
        AnalyticsService analyticsService = mock(AnalyticsService.class);
        UnsubscribeCommand command = new UnsubscribeCommand(broker, analyticsService);

        ClientConnection connection = mock(ClientConnection.class);
        UUID clientId = UUID.randomUUID();
        when(connection.getId()).thenReturn(clientId);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream("weather".getBytes(StandardCharsets.UTF_8)));
        byte[] response = command.execute(connection, in);

        verify(broker).unsubscribe(eq("weather"), eq(clientId.toString()));
        verify(analyticsService).recordUnsubscribe("weather");
        assertArrayEquals(new byte[0], response);
    }
}

