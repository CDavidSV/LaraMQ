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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class LaraMQClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String RED = "\u001B[31m";
    private static final String BLUE = "\u001B[34m";
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final Map<String, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
    private final Terminal terminal = TerminalBuilder.builder().build();
    private final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
    private final Map<String, Consumer<String[]>> commandHandlers = createCommandHandlers();

    LaraMQClient() throws IOException {
        socket = new Socket(Server.DOMAIN, Server.PORT);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
    }

    static void main(String[] args) throws IOException {
        LaraMQClient client = new LaraMQClient();
        client.log(LogLevel.SUCCESS, "Connected to LaraMQ broker");
        client.log(LogLevel.INFO, "Type 'help' for available commands or start typing commands");
        client.startReader();
        client.startREPL();
    }

    private void log(LogLevel level, String message) {
        String formatted = String.format("%s%s %s%s%s", level.color, level.symbol, message, RESET, "");
        reader.printAbove(formatted);
    }

    private void handleCommand(String[] parts) {
        if (parts.length == 0) {
            return;
        }

        String command = parts[0].toLowerCase(Locale.ROOT);

        Consumer<String[]> handler = commandHandlers.get(command);
        if (handler == null) {
            log(LogLevel.WARNING, "Unknown command: " + command);
            return;
        }

        handler.accept(parts);
    }

    private Map<String, Consumer<String[]>> createCommandHandlers() {
        Map<String, Consumer<String[]>> handlers = new HashMap<>();
        handlers.put("subscribe", this::handleSubscribeCommand);
        handlers.put("unsubscribe", this::handleUnsubscribeCommand);
        handlers.put("publish", this::handlePublishCommand);
        handlers.put("clear-retained", this::handleClearRetainedCommand);
        handlers.put("help", ignored -> printHelp());
        handlers.put("analytics", ignored -> getAndPrintAnalytics());
        handlers.put("list", ignored -> listTopics());
        handlers.put("exit", ignored -> handleExitCommand());
        return Map.copyOf(handlers);
    }

    private void handleSubscribeCommand(String[] parts) {
        if (parts.length < 2) {
            log(LogLevel.WARNING, "Usage: subscribe <topic>");
            return;
        }

        subscribeToTopic(parts[1]);
    }

    private void handleUnsubscribeCommand(String[] parts) {
        if (parts.length < 2) {
            log(LogLevel.WARNING, "Usage: unsubscribe <topic>");
            return;
        }

        unsubscribeFromTopic(parts[1]);
    }

    private void handlePublishCommand(String[] parts) {
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

    private void handleClearRetainedCommand(String[] parts) {
        if (parts.length < 2) {
            log(LogLevel.WARNING, "Usage: clear-retained <topic>");
            return;
        }

        clearRetainedMessage(parts[1]);
    }

    private void handleExitCommand() {
        log(LogLevel.INFO, "Exiting...");
        close();
        System.exit(0);
    }

    private void printHelp() {
        log(LogLevel.INFO, "Available commands:");
        log(LogLevel.INFO, "  subscribe <topic>          - Subscribe to a topic");
        log(LogLevel.INFO, "  unsubscribe <topic>        - Unsubscribe from a topic");
        log(LogLevel.INFO, "  publish <topic> <message> [retain] - Publish a message to a topic (use 'retain' to retain the message)");
        log(LogLevel.INFO, "  clear-retained <topic>     - Clear the retained message for a topic");
        log(LogLevel.INFO, "  analytics                  - Get analytics data");
        log(LogLevel.INFO, "  exit                       - Exit the client");
        log(LogLevel.INFO, "  help                       - Show this help message");
    }

    private void listTopics() {
        try {
            Frame response = callCommand(CommandCode.LIST.code, new byte[0]);
            String topicsJson = new String(response.payload(), StandardCharsets.UTF_8);
            String[] topics = MAPPER.readValue(topicsJson, String[].class);

            if (topics.length == 0) {
                log(LogLevel.INFO, "No topics available.");
            } else {
                log(LogLevel.INFO, "Available topics:");
                for (String topic : topics) {
                    log(LogLevel.INFO, "  - " + topic);
                }
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            log(LogLevel.ERROR, "Error listing topics: " + e.getMessage());
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
}
