package command;

import broker.Broker;
import org.junit.jupiter.api.Test;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PublishCommandTest {

    @Test
    void executePublishesPayloadAndRecordsAnalytics() throws Exception {
        Broker broker = mock(Broker.class);
        AnalyticsService analyticsService = mock(AnalyticsService.class);
        ClientConnection connection = mock(ClientConnection.class);
        PublishCommand command = new PublishCommand(broker, analyticsService);

        byte[] payload = "sunny".getBytes();
        DataInputStream input = publishPayload("weather", true, payload);

        byte[] response = command.execute(connection, input);

        verify(broker).publish(eq("weather"), eq(payload), eq(true));
        verify(analyticsService).recordPublish("weather");
        assertEquals(0, response.length);
        assertArrayEquals(new byte[0], response);
    }

    private static DataInputStream publishPayload(String topic, boolean retain, byte[] payload) throws Exception {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(raw);
        out.writeUTF(topic);
        out.writeBoolean(retain);
        out.write(payload);
        return new DataInputStream(new ByteArrayInputStream(raw.toByteArray()));
    }
}

