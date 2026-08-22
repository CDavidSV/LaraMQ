package command;

import broker.Broker;
import server.ClientConnection;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.logging.Logger;

public class PublishCommand extends Command {
    private static final Logger logger = Logger.getLogger(PublishCommand.class.getName());
    private final Broker broker;

    public PublishCommand(Broker broker) {
        this.broker = broker;
        logger.info("Publish command registered");
    }

    @Override
    public byte[] execute(ClientConnection conn, DataInputStream in) throws IOException {
        String topicName = in.readUTF();
        boolean retain = in.readBoolean();
        byte[] payload = in.readAllBytes();


        broker.publish(topicName, payload, retain);
        return new byte[0];
    }
}
