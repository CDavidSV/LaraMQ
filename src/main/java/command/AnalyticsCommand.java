package command;

import com.fasterxml.jackson.databind.ObjectMapper;
import server.ClientConnection;
import services.analytics.AnalyticsService;
import services.analytics.AnalyticsSnapshot;

import java.io.DataInputStream;
import java.io.IOException;

public class AnalyticsCommand extends Command {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final AnalyticsService analyticsService;

    public AnalyticsCommand(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @Override
    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        // For simplicity, we will ignore the input and return the analytics data as a string
        AnalyticsSnapshot analyticsData = analyticsService.getSnapshot();

        // Serialize the analytics data to JSON
        String json = MAPPER.writeValueAsString(analyticsData);
        return json.getBytes();
    }
}
