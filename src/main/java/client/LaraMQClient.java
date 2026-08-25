package client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import command.CommandCode;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import protocol.*;
import server.Server;
import services.analytics.AnalyticsSnapshot;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class LaraMQClient {
    private static final Path DEFAULT_CLIENT_CONFIG_PATH = Path.of("data", "client_config.json");

    enum LogLevel {
        SUCCESS(GREEN, "✓"),
        INFO(BLUE, "ℹ"),
        WARNING(YELLOW, "⚠"),
        ERROR(RED, "✗"),
        NOTIFICATION(CYAN, "⟲");

        final String color;
        final String symbol;

        LogLevel(String color, String symbol) {
            this.color = color;
            this.symbol = symbol;
        }
    }

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final UUID clientId;
    private final Map<String, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
    private final Terminal terminal = TerminalBuilder.builder().build();
    private final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String RED = "\u001B[31m";
    private static final String BLUE = "\u001B[34m";

    private record ClientConfig(String clientId) {
    }

    private void log(LogLevel level, String message) {
        String formatted = String.format("%s%s %s%s%s", level.color, level.symbol, message, RESET, "");
        reader.printAbove(formatted);
    }

    LaraMQClient(UUID clientId) throws IOException {
        this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");
        socket = new Socket(Server.DOMAIN, Server.PORT);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
        authenticate();
    }

    private void authenticate() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream payloadOut = new DataOutputStream(buf);
        payloadOut.writeUTF(clientId.toString());
        FrameWriter.writeFrame(out, MessageCode.AUTHENTICATE.code, UUID.randomUUID(), buf.toByteArray());
    }

    static UUID loadOrCreateClientId(Path configFile) throws IOException {
        Path normalizedConfigFile = configFile.toAbsolutePath().normalize();
        Path parent = normalizedConfigFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        if (Files.exists(normalizedConfigFile) && Files.size(normalizedConfigFile) > 0L) {
            try {
                ClientConfig config = MAPPER.readValue(normalizedConfigFile.toFile(), ClientConfig.class);
                if (config != null && config.clientId() != null && !config.clientId().isBlank()) {
                    return UUID.fromString(config.clientId());
                }
            } catch (JsonProcessingException | IllegalArgumentException e) {
                // Regenerate and overwrite invalid config content.
            }
        }

        UUID generatedClientId = UUID.randomUUID();
        writeClientConfig(normalizedConfigFile, generatedClientId);
        return generatedClientId;
    }

    private static void writeClientConfig(Path configFile, UUID clientId) throws IOException {
        Path tmpFile = configFile.resolveSibling(configFile.getFileName() + ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmpFile.toFile(), new ClientConfig(clientId.toString()));
        Files.move(tmpFile, configFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void handleCommand(String[] parts) {
        if (parts.length == 0) {
            return;
        }

        String command = parts[0].toLowerCase(Locale.ROOT);

        switch (command) {
            case "subscribe" -> {
                if (parts.length < 2) {
                    log(LogLevel.WARNING, "Usage: subscribe <topic>");
                    return;
                }
                String topic = parts[1];
                subscribeToTopic(topic);
            }
            case "unsubscribe" -> {
                if (parts.length < 2) {
                    log(LogLevel.WARNING, "Usage: unsubscribe <topic>");
                    return;
                }
                String topic = parts[1];
                unsubscribeFromTopic(topic);
            }
            case "publish" -> {
                if (parts.length < 3) {
                    log(LogLevel.WARNING, "Usage: publish <topic> <message> [retain]");
                    return;
                }

                boolean retain = parts[parts.length - 1].toLowerCase(Locale.ROOT).equalsIgnoreCase("retain");

                String topic = parts[1];
                String message = String.join(" ", java.util.Arrays.copyOfRange(parts, 2, parts.length - (retain ? 1 : 0)));

                if (message.isEmpty()) {
                    log(LogLevel.WARNING, "Message cannot be empty");
                    return;
                }

                publishToTopic(topic, message.getBytes(StandardCharsets.UTF_8), retain);
            }
            case "clear-retained" -> {
                if (parts.length < 2) {
                    log(LogLevel.WARNING, "Usage: clear-retained <topic>");
                    return;
                }
                String topic = parts[1];
                clearRetainedMessage(topic);
            }
            case "help" -> {
                log(LogLevel.INFO, "Available commands:");
                log(LogLevel.INFO, "  subscribe <topic>          - Subscribe to a topic");
                log(LogLevel.INFO, "  unsubscribe <topic>        - Unsubscribe from a topic");
                log(LogLevel.INFO, "  publish <topic> <message> [retain] - Publish a message to a topic (use 'retain' to retain the message)");
                log(LogLevel.INFO, "  clear-retained <topic>     - Clear the retained message for a topic");
                log(LogLevel.INFO, "  analytics                  - Get analytics data");
                log(LogLevel.INFO, "  exit                       - Exit the client");
                log(LogLevel.INFO, "  help                       - Show this help message");
            }
            case "exit" -> {
                log(LogLevel.INFO, "Exiting...");
                close();
                System.exit(0);
            }
            case "analytics" -> getAndPrintAnalytics();
            default -> log(LogLevel.WARNING, "Unknown command: " + command);
        }
    }

    private void handleNewNotification(Frame frame) {
        try {
            DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(frame.payload()));
            String topic = payloadIn.readUTF();
            byte[] payload = payloadIn.readAllBytes();
            log(LogLevel.NOTIFICATION, "New notification on topic [" + topic + "]: " + new String(payload, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log(LogLevel.ERROR, "Error reading payload: " + e.getMessage());
        }
    }

    private synchronized Frame callCommand(byte commandCode, byte[] payload) throws IOException, ExecutionException, InterruptedException {
        UUID requestId = UUID.randomUUID();
        CompletableFuture<Frame> future = new CompletableFuture<>();
        pending.put(requestId.toString(), future);

        // Prepend the CommandCode byte to the payload so the server can distinguish
        // the command type independently of the frame's MessageCode.
        ByteArrayOutputStream buf = new ByteArrayOutputStream(1 + payload.length);
        buf.write(commandCode);
        buf.write(payload);

        FrameWriter.writeFrame(out, MessageCode.COMMAND.code, requestId, buf.toByteArray());
        return future.get();
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
                } catch (SocketException e) {
                    log(LogLevel.INFO, "Connection closed");
                    return;
                } catch (IOException | ProtocolException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    public void startREPL() {
        while (true) {
            String line = reader.readLine("LaraMQ> ");
            String[] parts = line.trim().split("\\s+");
            handleCommand(parts);
        }
    }

    public synchronized void subscribeToTopic(String topic) {
        try {
            Frame response = callCommand(CommandCode.SUBSCRIBE.code, topic.getBytes(StandardCharsets.UTF_8));
            byte[] retainedMessage = response.payload();

            if (retainedMessage.length > 0) {
                log(LogLevel.SUCCESS, "Subscribed to topic [" + topic + "]");
                log(LogLevel.INFO, "Retained message: " + new String(retainedMessage, StandardCharsets.UTF_8));
            } else {
                log(LogLevel.SUCCESS, "Subscribed to topic [" + topic + "] - No retained message");
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            log(LogLevel.ERROR, "Error subscribing to topic [" + topic + "]: " + e.getMessage());
        }
    }

    public synchronized void unsubscribeFromTopic(String topic) {
        try {
            callCommand(CommandCode.UNSUBSCRIBE.code, topic.getBytes(StandardCharsets.UTF_8));
            log(LogLevel.SUCCESS, "Unsubscribed from topic [" + topic + "]");
        } catch (IOException | ExecutionException | InterruptedException e) {
            log(LogLevel.ERROR, "Error unsubscribing from topic [" + topic + "]: " + e.getMessage());
        }
    }

    public synchronized void publishToTopic(String topic, byte[] payload, boolean retain) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);

        try {
            dos.writeUTF(topic);
            dos.writeBoolean(retain);
            dos.write(payload);

            callCommand(CommandCode.PUBLISH.code, buf.toByteArray());
            String retainStr = retain ? " (retained)" : "";
            log(LogLevel.SUCCESS, "Published to topic [" + topic + "]" + retainStr);
        } catch (IOException | ExecutionException | InterruptedException e) {
            log(LogLevel.ERROR, "Error publishing to topic [" + topic + "]: " + e.getMessage());
        }
    }

    public void clearRetainedMessage(String topic) {
        publishToTopic(topic, new byte[0], true);
    }

    public synchronized AnalyticsSnapshot getAnalyticsData() throws IOException, ExecutionException, InterruptedException {
        Frame responseFrame = callCommand(CommandCode.ANALYTICS.code, new byte[0]);
        String jsonString = new String(responseFrame.payload(), StandardCharsets.UTF_8);
        return MAPPER.readValue(jsonString, AnalyticsSnapshot.class);

    }

    public void getAndPrintAnalytics() {
        try {
            AnalyticsSnapshot snapshot = getAnalyticsData();
            log(LogLevel.INFO, "Analytics Data:");
            log(LogLevel.INFO, snapshot.toString());
        } catch (IOException | ExecutionException | InterruptedException e) {
            log(LogLevel.ERROR, "Error fetching analytics: " + e.getMessage());
        }
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            log(LogLevel.ERROR, "Error closing connection: " + e.getMessage());
        }
    }


    static void main() throws IOException {
        UUID clientId = loadOrCreateClientId(DEFAULT_CLIENT_CONFIG_PATH);
        LaraMQClient client = new LaraMQClient(clientId);
        client.log(LogLevel.SUCCESS, "Connected to LaraMQ broker as client [" + clientId + "]");
        client.log(LogLevel.INFO, "Type 'help' for available commands or start typing commands");
        client.startReader();
        client.startREPL();
    }
}
