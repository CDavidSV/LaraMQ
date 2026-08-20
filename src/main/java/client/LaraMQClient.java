package client;

import command.CommandCode;
import protocol.*;
import server.Server;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class LaraMQClient {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Map<String, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();

    LaraMQClient() throws IOException {
        socket = new Socket(Server.DOMAIN, Server.PORT);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
    }

    public void startReader() {
        new Thread(() -> {
            while(!socket.isClosed()) {
                try {
                    Frame frame = FrameReader.readFrame(in);

                    if (frame.type() == MessageCode.NOTIFICATION.code) {
                        handleNewNotification(frame);
                        continue;
                    }

                    CompletableFuture<Frame> future = pending.remove(frame.id().toString());
                    if (future != null) {
                        future.complete(frame);
                    }
                } catch (IOException | ProtocolException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    public void handleNewNotification(Frame frame) {
        try {
            DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(frame.payload()));
            String topic = payloadIn.readUTF();
            byte[] payload = payloadIn.readAllBytes();
            System.out.println("Received notification on topic " + topic + ": " + new String(payload, StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("Error reading payload: " + e.getMessage());
        }
    }

    public synchronized void subscribeToTopic(String topic) {
        try {
            UUID requestId = UUID.randomUUID();

            CompletableFuture<Frame> future = new CompletableFuture<>();
            pending.put(requestId.toString(), future);
            FrameWriter.writeFrame(out, CommandCode.SUBSCRIBE.code, requestId, topic.getBytes(StandardCharsets.UTF_8));
            future.get();
            System.out.println("Subscribed to topic " + topic);
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.err.println("Error subscribing to topic " + topic);
            e.printStackTrace();
        }
    }

    public synchronized void publishToTopic(String topic, byte[] payload) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);

        try {
            dos.writeUTF(topic);
            dos.write(payload);

            CompletableFuture<Frame> future = new CompletableFuture<>();
            UUID requestId = UUID.randomUUID();
            pending.put(requestId.toString(), future);
            FrameWriter.writeFrame(out, CommandCode.PUBLISH.code, requestId, buf.toByteArray());
            future.get();
            System.out.println("Published to topic " + topic);
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.err.println("Error publishing to topic " + topic);
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

    static void main() throws IOException, InterruptedException {
        LaraMQClient client = new LaraMQClient();
        client.startReader();

        client.subscribeToTopic("test");
        Thread.sleep(2000);
        client.publishToTopic("test", "Hello World!".getBytes(StandardCharsets.UTF_8));

        Thread.sleep(50000);
        client.close();
    }
}
