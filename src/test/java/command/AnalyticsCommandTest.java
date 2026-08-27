package command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import server.ClientConnection;
import services.analytics.AnalyticsService;
import services.analytics.AnalyticsSnapshot;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AnalyticsCommandTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void executeSerializesSnapshotToJson() throws Exception {
        AnalyticsService analyticsService = mock(AnalyticsService.class);
        AnalyticsSnapshot snapshot = new AnalyticsSnapshot(
                1000L,
                1L,
                2L,
                3L,
                4L,
                5L,
                6L,
                Map.of("weather", 7L),
                Map.of("weather", 8L),
                Map.of("PUBLISH", 9L)
        );
        when(analyticsService.getSnapshot()).thenReturn(snapshot);

        AnalyticsCommand command = new AnalyticsCommand(analyticsService);
        ClientConnection connection = mock(ClientConnection.class);

        byte[] response = command.execute(connection, new DataInputStream(new ByteArrayInputStream(new byte[0])));
        JsonNode json = MAPPER.readTree(new String(response, StandardCharsets.UTF_8));

        assertEquals(1L, json.get("totalPublishes").asLong());
        assertEquals(7L, json.get("publishCountByTopic").get("weather").asLong());
        assertEquals(9L, json.get("commandCountByType").get("PUBLISH").asLong());
    }
}

