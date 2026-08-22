package client;

import com.fasterxml.jackson.databind.ObjectMapper;
import command.CommandCode;
import protocol.*;
import server.Server;
import services.analytics.AnalyticsSnapshot;

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
    private static final ObjectMapper MAPPER = new ObjectMapper();

    LaraMQClient() throws IOException {
        socket = new Socket(Server.DOMAIN, Server.PORT);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
    }

    public void startReader() {
        new Thread(() -> {
            while (!socket.isClosed()) {
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
            byte[] retainedMessage = future.get().payload();

            if (retainedMessage.length > 0) {
                System.out.println("Subscribed to topic " + topic + ".\nRetained message: " + new String(retainedMessage, StandardCharsets.UTF_8));
            } else {
                System.out.println("Subscribed to topic " + topic + ". No retained message.");
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.err.println("Error subscribing to topic " + topic);
            e.printStackTrace();
        }
    }

    public synchronized void unsubscribeFromTopic(String topic) {
        try {
            UUID requestId = UUID.randomUUID();

            CompletableFuture<Frame> future = new CompletableFuture<>();
            pending.put(requestId.toString(), future);
            FrameWriter.writeFrame(out, CommandCode.UNSUBSCRIBE.code, requestId, topic.getBytes(StandardCharsets.UTF_8));
            future.get();
            System.out.println("Unsubscribed from topic " + topic);
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.err.println("Error unsubscribing from topic " + topic);
            e.printStackTrace();
        }
    }

    public synchronized void publishToTopic(String topic, byte[] payload, boolean retain) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);

        try {
            dos.writeUTF(topic);
            dos.writeBoolean(retain);
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

    public void publishRetainedMessage(String topic, byte[] payload) {
        publishToTopic(topic, payload, true);
    }

    public void clearRetainedMessage(String topic) {
        publishToTopic(topic, new byte[0], true);
    }

    public synchronized AnalyticsSnapshot getAnalyticsData() throws IOException, ExecutionException, InterruptedException {
        UUID requestId = UUID.randomUUID();

        CompletableFuture<Frame> future = new CompletableFuture<>();
        pending.put(requestId.toString(), future);
        FrameWriter.writeFrame(out, CommandCode.ANALYTICS.code, requestId, new byte[0]);
        Frame responseFrame = future.get();

        String jsonString = new String(responseFrame.payload(), StandardCharsets.UTF_8);
        return MAPPER.readValue(jsonString, AnalyticsSnapshot.class);

    }

    public void getAndPrintAnalytics() {
        try {
            AnalyticsSnapshot snapshot = getAnalyticsData();
            System.out.println(snapshot);
        } catch (IOException | ExecutionException | InterruptedException e) {
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
        client.publishRetainedMessage("test", "Hello World!".getBytes(StandardCharsets.UTF_8));
        client.getAndPrintAnalytics();
//        Thread.sleep(2000);
//        client.unsubscribeFromTopic("test");
//        client.subscribeToTopic("test");
//        Thread.sleep(2000);
//        client.clearRetainedMessage("test");
//        client.unsubscribeFromTopic("test");
//        client.subscribeToTopic("test");

        Thread.sleep(10000);
        client.close();
    }
}
