package server;

import broker.Broker;
import broker.store.TopicDataStore;
import command.CommandRegistry;
import protocol.*;
import services.analytics.AnalyticsService;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server implements AutoCloseable {
    public static final String DOMAIN = "127.0.0.1";
    public static final int PORT = 3000;
    private static final Logger logger = Logger.getLogger(Server.class.getName());
    private final String host;
    private final ServerSocket server;
    private final ExecutorService executor;
    private final CommandRegistry commandRegistry;
    private final AnalyticsService analyticsService = new AnalyticsService();
    private final ClientSessionHandler clientSessionHandler = new ClientSessionHandler(ServerBootstrap.DEFAULT_CLIENT_SESSION_DATA_FILE_PATH);
    private final Broker broker = new Broker(new TopicDataStore(ServerBootstrap.DEFAULT_TOPIC_DATA_FILE_PATH), analyticsService, clientSessionHandler);
    private volatile boolean running = true;

    public Server(int port) throws IOException {
        this(DOMAIN, port);
    }

    public Server(String host, int port) throws IOException {
        this.host = host;
        server = new ServerSocket(port, 50, InetAddress.getByName(host));
        executor = Executors.newVirtualThreadPerTaskExecutor();

        commandRegistry = ServerBootstrap.buildRegistry(broker, analyticsService);
    }

    static void main() throws IOException {
        try (Server server = new Server(Server.PORT)) {
            server.start();
        }
    }

    public void start() {
        logger.info("Starting server on %s:%d".formatted(host, server.getLocalPort()));

        while (running) {
            try {
                Socket clientSocket = server.accept();
                executor.submit(() -> handleNewClient(clientSocket));
            } catch (IOException e) {
                if (running) {
                    logger.log(Level.WARNING, "failed to accept client", e);
                }
            }
        }
    }

    private AuthenticationRequest authenticateClient(DataInputStream in) throws IOException, ProtocolException, AuthenticationException {
        Frame frame = FrameReader.readFrame(in);

        if (frame.type() != MessageCode.AUTHENTICATE.code) {
            throw new ProtocolException("Expected AUTHENTICATE message, got: " + frame.type());
        }

        DataInputStream authenticationPayload = new DataInputStream(new ByteArrayInputStream(frame.payload()));
        String clientIdString = authenticationPayload.readUTF();
        boolean clearSessionIfExists = false;
        if (authenticationPayload.available() > 0) {
            clearSessionIfExists = authenticationPayload.readBoolean();
        }

        try {
            return new AuthenticationRequest(UUID.fromString(clientIdString), clearSessionIfExists);
        } catch (IllegalArgumentException e) {
            throw new AuthenticationException("Invalid UUID format: " + clientIdString);
        }
    }

    private void handleNewClient(Socket clientSocket) {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
            AuthenticationRequest authRequest = authenticateClient(in);
            UUID clientId = authRequest.clientId();

            ClientSession existingSession = clientSessionHandler.get(clientId.toString());
            if (authRequest.clearSessionIfExists() && existingSession != null) {
                broker.unsubscribeAll(clientId.toString());
                existingSession.clearSessionData();
                logger.info("Session reset for client (ID: %s) during authentication".formatted(clientId));
            }

            ClientSession session = existingSession != null ? existingSession : clientSessionHandler.getOrCreate(clientId);

            ClientConnection existingConnection = session.getClientConnection();
            if (existingConnection != null) {
                logger.info("Client (ID: %s) is already connected. Closing the existing connection.".formatted(clientId));
                existingConnection.close();
            }

            ClientConnection clientConn = new ClientConnection(clientSocket, in, commandRegistry, analyticsService, clientId, session);
            session.setClientConnection(clientConn);
            analyticsService.recordClientConnected();
            logger.info("new client connected (ID: %s)".formatted(clientConn.getId()));

            session.flushUndeliveredMessages();

            try {
                clientConn.reader();
            } catch (ProtocolException e) {
                logger.log(Level.WARNING, "Malformed frame, closing connection (ID: %s): %s".formatted(clientConn.getId(), e.getMessage()));
            } catch (EOFException e) {
                logger.info("client disconnected (ID: %s)".formatted(clientConn.getId()));
            } finally {
                // broker.unsubscribeAll(clientId.toString());
                session.setClientConnection(null);
                clientConn.close();
                analyticsService.recordClientDisconnected();
            }
        } catch (AuthenticationException e) {
            logger.log(Level.WARNING, "authentication failed", e);
        } catch (IOException | ProtocolException e) {
            logger.log(Level.WARNING, "client error", e);
        }
    }

    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }

        running = false;
        try {
            broker.shutdown();
            clientSessionHandler.close();
            server.close();
            executor.shutdown();
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    private record AuthenticationRequest(UUID clientId, boolean clearSessionIfExists) {
    }
}
