package command;

import broker.Broker;
import broker.Topic;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class SubscribeCommand extends Command {
    private static final Logger logger = Logger.getLogger(SubscribeCommand.class.getName());
    private final Broker broker;
    private final AnalyticsService analyticsService;

    public SubscribeCommand(Broker broker, AnalyticsService analyticsService) {
        this.broker = broker;
        this.analyticsService = analyticsService;
        logger.info("Subscribe command registered");
    }

    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        String topicName = new String(
                in.readAllBytes(),
                StandardCharsets.UTF_8
        );

        Topic topic = broker.subscribe(topicName, conn);
        byte[] retainedMessage = topic.getRetainedMessage();
        analyticsService.recordSubscribe(topicName);
        return retainedMessage == null ? new byte[0] : retainedMessage;
    }
}
