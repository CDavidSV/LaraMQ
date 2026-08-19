package client;

import command.Command;
import command.CommandCode;
import command.CommandException;
import protocol.Frame;
import protocol.FrameReader;
import protocol.FrameWriter;
import protocol.ProtocolException;
import server.Server;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class LaraMQClient {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    LaraMQClient() throws IOException {
        socket = new Socket(Server.DOMAIN, Server.PORT);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
    }

    public void reader() throws ProtocolException, IOException {
        while(!socket.isClosed()) {
            Frame frame = FrameReader.readFrame(in);
            System.out.println(frame);
        }
    }

    public void subscribeToTopic(String topic) {
        try {
            FrameWriter.writeFrame(out, CommandCode.SUBSCRIBE.code, UUID.randomUUID(), topic.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LaraMQClient client = new LaraMQClient();

        client.subscribeToTopic("test");
        Thread.sleep(50000);
    }
}
