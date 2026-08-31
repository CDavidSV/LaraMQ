package server;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientSessionHandler implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ClientSessionHandler.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentMap<String, ClientSession> clientSessions = new ConcurrentHashMap<>();
    private final BlockingQueue<Boolean> queue = new LinkedBlockingQueue<>(1);
    private final ExecutorService writer = Executors.newSingleThreadExecutor();
    private final Path file;
    private final Object ioLock = new Object();
    private volatile boolean closing;

    public ClientSessionHandler(Path file) {
        this.file = file;

        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory for " + file, e);
        }

        try {
            ClientSessionData[] initialData = readFromFile();
            for (ClientSessionData sessionData : initialData) {
                clientSessions.put(
                        sessionData.clientId().toString(),
                        ClientSession.fromData(sessionData, () -> markDirty(sessionData.clientId().toString()))
                );
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read client session data for " + file, e);
        }

        writer.submit(this::processWrites);
    }

    public ClientSession get(String clientId) {
        return clientSessions.get(clientId);
    }

    public ClientSession[] getAll() {
        return clientSessions.values().toArray(new ClientSession[0]);
    }

    public ClientSession getOrCreate(UUID clientId) {
        return clientSessions.computeIfAbsent(clientId.toString(), id -> {
            ClientSession session = new ClientSession(clientId, () -> markDirty(id));
            markDirty(id);
            return session;
        });
    }

    public void remove(String clientId) {
        ClientSession removed = clientSessions.remove(clientId);
        if (removed != null && !closing) {
            queue.offer(true);
        }
    }

    public void markDirty(String clientId) {
        if (closing) {
            return;
        }

        ClientSession session = clientSessions.get(clientId);
        if (session != null) {
            queue.offer(true);
        }
    }

    @Override
    public void close() {
        closing = true;
        writer.shutdownNow();

        try {
            if (!writer.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warning("Timed out waiting for client session writer thread to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            writeToFile(snapshotData());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write client session data during shutdown", e);
        }
    }

    private void processWrites() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                queue.take();
                writeToFile(snapshotData());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to write client session data", e);
                if (!closing) {
                    queue.offer(true);
                }
            }
        }
    }

    private Map<String, ClientSessionData> snapshotData() {
        Map<String, ClientSessionData> data = new ConcurrentHashMap<>();

        clientSessions.forEach((clientId, session) -> data.put(clientId, session.toData()));

        return Map.copyOf(data);
    }

    private void writeToFile(Map<String, ClientSessionData> data) throws IOException {
        synchronized (ioLock) {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writeValue(tmp.toFile(), data.values());
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private ClientSessionData[] readFromFile() throws IOException {
        if (!Files.exists(file)) {
            return new ClientSessionData[0];
        }

        return MAPPER.readValue(file.toFile(), ClientSessionData[].class);
    }
}