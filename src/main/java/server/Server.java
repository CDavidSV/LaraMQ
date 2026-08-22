package server;

import broker.Broker;
import broker.store.FileTopicDataStore;
import command.CommandRegistry;
import protocol.ProtocolException;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(Server.class.getName());
    public static final String DOMAIN = "127.0.0.1";
    public static final int PORT = 3000;

    private final String host;
    private final ServerSocket server;
    private final ExecutorService executor;
    private volatile boolean running = true;
    private final CommandRegistry commandRegistry;
    private final Broker broker = new Broker(new FileTopicDataStore(ServerBootstrap.DEFAULT_TOPIC_DATA_FILE_PATH));

    public Server(int port) throws IOException {
        this(DOMAIN, port);
    }

    public Server(String host, int port) throws IOException {
        this.host = host;
        server = new ServerSocket(port, 50, InetAddress.getByName(host));
        executor = Executors.newVirtualThreadPerTaskExecutor();

        commandRegistry = ServerBootstrap.buildRegistry(broker);
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

    private void handleNewClient(Socket clientSocket) {
        try {
            ClientConnection clientConn = new ClientConnection(clientSocket, commandRegistry);
            logger.info("new client connected (ID: %s)".formatted(clientConn.getId()));

            try {
                clientConn.reader();
            } catch (ProtocolException e) {
                logger.log(Level.WARNING, "Malformed frame, closing connection (ID: %s): %s".formatted(clientConn.getId(), e.getMessage()));
            } catch (EOFException e) {
                logger.info("client disconnected (ID: %s)".formatted(clientConn.getId()));
            } finally {
                broker.unsubscribeAll(clientConn);
                clientConn.close();
            }
        } catch (IOException e) {
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
            server.close();
            executor.shutdown();
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    static void main() throws IOException {
        try (Server server = new Server(Server.PORT)) {
            server.start();
        }
    }
}
