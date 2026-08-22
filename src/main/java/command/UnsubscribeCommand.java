package command;

import broker.Broker;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class UnsubscribeCommand extends Command {
    private static final Logger logger = Logger.getLogger(UnsubscribeCommand.class.getName());
    private final Broker broker;
    private final AnalyticsService analyticsService;

    public UnsubscribeCommand(Broker broker, AnalyticsService analyticsService) {
        this.broker = broker;
        this.analyticsService = analyticsService;
        logger.info("Unsubscribe command registered");
    }

    @Override
    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        String topic = new String(
                in.readAllBytes(),
                StandardCharsets.UTF_8
        );

        broker.unsubscribe(topic, conn);
        analyticsService.recordUnsubscribe(topic);
        return new byte[0];
    }
}
