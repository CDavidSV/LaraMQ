package command;

import broker.Broker;

import java.io.DataInputStream;

public class ListCommand extends Command {
    private final Broker broker;

    public ListCommand(Broker broker) {
        this.broker = broker;
    }

    @Override
    public byte[] execute(server.ClientConnection conn, DataInputStream in) {
        String[] topics = broker.listTopics();

        return String.join(", ", topics).getBytes();
    }
}
