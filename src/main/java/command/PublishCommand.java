package command;

import broker.Broker;
import server.ClientConnection;
import services.analytics.AnalyticsService;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.logging.Logger;

public class PublishCommand extends Command {
    private static final Logger logger = Logger.getLogger(PublishCommand.class.getName());
    private final Broker broker;
    private final AnalyticsService analyticsService;

    public PublishCommand(Broker broker, AnalyticsService analyticsService) {
        this.broker = broker;
        this.analyticsService = analyticsService;
        logger.info("Publish command registered");
    }

    @Override
    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        String topicName = in.readUTF();
        boolean retain = in.readBoolean();
        byte[] payload = in.readAllBytes();

        broker.publish(topicName, payload, retain);
        analyticsService.recordPublish(topicName);
        return new byte[0];
    }
}
