package command;

import broker.Broker;
import server.ClientConnection;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class UnsubscribeCommand extends Command {
    private static final Logger logger = Logger.getLogger(UnsubscribeCommand.class.getName());
    private final Broker broker;

    public UnsubscribeCommand(Broker broker) {
        this.broker = broker;
        logger.info("Unsubscribe command registered");
    }

    @Override
    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        String topic = new String(
                in.readAllBytes(),
                StandardCharsets.UTF_8
        );

        broker.unsubscribe(topic, conn);
        return new byte[0];
    }
}
