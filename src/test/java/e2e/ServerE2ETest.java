package e2e;

import command.CommandCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import protocol.Frame;
import protocol.FrameReader;
import protocol.FrameWriter;
import protocol.MessageCode;
import protocol.ProtocolException;
import server.Server;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerE2ETest {

    private static final String HOST = "127.0.0.1";
    private static final List<Path> PERSISTED_DATA_FILES = List.of(
            Path.of("data", "topic_data.json"),
            Path.of("data", "client_sessions.json")
    );

    private final Map<Path, byte[]> backup = new HashMap<>();
    private final Set<Path> originallyMissing = new HashSet<>();

    private Server server;
    private Thread serverThread;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        backupAndResetDataFiles();

        port = findFreePort();
        server = new Server(HOST, port);
        serverThread = Thread.startVirtualThread(server::start);

        waitForServerReady();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }

        if (serverThread != null) {
            serverThread.join(1500);
        }

        restoreDataFiles();
    }

    @Test
    void publishAfterSubscribeDeliversNotificationToSubscriber() throws Exception {
        UUID subscriberId = UUID.randomUUID();
        UUID publisherId = UUID.randomUUID();
        String topic = "e2e/weather/" + UUID.randomUUID();
        byte[] payload = "sunny".getBytes(StandardCharsets.UTF_8);

        try (ClientSession subscriber = connectClient(subscriberId, false);
             ClientSession publisher = connectClient(publisherId, false)) {

            sendSubscribe(subscriber.out(), topic);
            Frame subscribeAck = readFrame(subscriber.in());
            assertEquals(MessageCode.ACK.code, subscribeAck.type());

            sendPublish(publisher.out(), topic, false, payload);
            Frame publishAck = readFrame(publisher.in());
            assertEquals(MessageCode.ACK.code, publishAck.type());

            Frame notification = readFrame(subscriber.in());
            assertEquals(MessageCode.NOTIFICATION.code, notification.type());

            DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(notification.payload()));
            String deliveredTopic = payloadIn.readUTF();
            byte[] deliveredPayload = payloadIn.readAllBytes();

            assertEquals(topic, deliveredTopic);
            assertArrayEquals(payload, deliveredPayload);
        }
    }

    @Test
    void retainedMessageIsReturnedInSubscribeAck() throws Exception {
        UUID publisherId = UUID.randomUUID();
        UUID subscriberId = UUID.randomUUID();
        String topic = "e2e/retain/" + UUID.randomUUID();
        byte[] retained = "retained-message".getBytes(StandardCharsets.UTF_8);

        try (ClientSession publisher = connectClient(publisherId, false)) {
            sendPublish(publisher.out(), topic, true, retained);
            Frame publishAck = readFrame(publisher.in());
            assertEquals(MessageCode.ACK.code, publishAck.type());
        }

        try (ClientSession subscriber = connectClient(subscriberId, false)) {
            sendSubscribe(subscriber.out(), topic);
            Frame subscribeAck = readFrame(subscriber.in());

            assertEquals(MessageCode.ACK.code, subscribeAck.type());
            assertArrayEquals(retained, subscribeAck.payload());
        }
    }

    private ClientSession connectClient(UUID clientId, boolean clearSessionIfExists) throws IOException {
        Socket socket = new Socket(HOST, port);
        socket.setSoTimeout(3000);

        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        ByteArrayOutputStream authPayloadBuffer = new ByteArrayOutputStream();
        DataOutputStream authPayloadOut = new DataOutputStream(authPayloadBuffer);
        authPayloadOut.writeUTF(clientId.toString());
        authPayloadOut.writeBoolean(clearSessionIfExists);

        FrameWriter.writeFrame(out, MessageCode.AUTHENTICATE.code, UUID.randomUUID(), authPayloadBuffer.toByteArray());

        return new ClientSession(socket, in, out);
    }

    private static void sendSubscribe(DataOutputStream out, String topic) throws IOException {
        sendCommand(out, CommandCode.SUBSCRIBE.code, topic.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendPublish(DataOutputStream out, String topic, boolean retain, byte[] payload) throws IOException {
        ByteArrayOutputStream commandPayloadBuffer = new ByteArrayOutputStream();
        DataOutputStream commandPayloadOut = new DataOutputStream(commandPayloadBuffer);
        commandPayloadOut.writeUTF(topic);
        commandPayloadOut.writeBoolean(retain);
        commandPayloadOut.write(payload);

        sendCommand(out, CommandCode.PUBLISH.code, commandPayloadBuffer.toByteArray());
    }

    private static void sendCommand(DataOutputStream out, byte commandCode, byte[] commandPayload) throws IOException {
        ByteArrayOutputStream framePayloadBuffer = new ByteArrayOutputStream();
        framePayloadBuffer.write(commandCode);
        framePayloadBuffer.write(commandPayload);

        FrameWriter.writeFrame(out, MessageCode.COMMAND.code, UUID.randomUUID(), framePayloadBuffer.toByteArray());
    }

    private static Frame readFrame(DataInputStream in) throws IOException, ProtocolException {
        return FrameReader.readFrame(in);
    }

    private void waitForServerReady() throws Exception {
        long deadline = System.currentTimeMillis() + 3000;

        while (System.currentTimeMillis() < deadline) {
            try (Socket probe = new Socket(HOST, port)) {
                DataOutputStream out = new DataOutputStream(probe.getOutputStream());
                ByteArrayOutputStream authPayloadBuffer = new ByteArrayOutputStream();
                DataOutputStream authPayloadOut = new DataOutputStream(authPayloadBuffer);
                authPayloadOut.writeUTF(UUID.randomUUID().toString());
                authPayloadOut.writeBoolean(false);
                FrameWriter.writeFrame(out, MessageCode.AUTHENTICATE.code, UUID.randomUUID(), authPayloadBuffer.toByteArray());
                return;
            } catch (IOException ignored) {
                Thread.sleep(25);
            }
        }

        assertTrue(false, "Server did not become ready in time");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getByName(HOST))) {
            return socket.getLocalPort();
        }
    }

    private void backupAndResetDataFiles() throws IOException {
        for (Path file : PERSISTED_DATA_FILES) {
            Path absolute = file.toAbsolutePath();
            Path parent = absolute.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            if (Files.exists(absolute)) {
                backup.put(absolute, Files.readAllBytes(absolute));
                Files.delete(absolute);
            } else {
                originallyMissing.add(absolute);
            }
        }
    }

    private void restoreDataFiles() throws IOException {
        for (Path file : PERSISTED_DATA_FILES) {
            Path absolute = file.toAbsolutePath();
            byte[] previous = backup.get(absolute);

            if (previous != null) {
                Files.write(absolute, previous);
            } else if (originallyMissing.contains(absolute) && Files.exists(absolute)) {
                Files.delete(absolute);
            }
        }
    }

    private record ClientSession(Socket socket, DataInputStream in, DataOutputStream out) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}


