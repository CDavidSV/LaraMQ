package command;

import server.ClientConnection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SubscribeCommand extends Command {
    public void execute(ClientConnection conn, DataInputStream in, DataOutputStream out) throws IOException {

        String topic = new String(
            in.readAllBytes(),
            StandardCharsets.UTF_8
        );

        System.out.println("Subscribing to topic: " + topic);
    }
}
