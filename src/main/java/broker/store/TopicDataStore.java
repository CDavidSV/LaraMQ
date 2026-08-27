package broker.store;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TopicDataStore {
    private static final Logger logger = Logger.getLogger(TopicDataStore.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, TopicData> data = new ConcurrentHashMap<>();
    private final BlockingQueue<Boolean> queue = new LinkedBlockingQueue<>(1);
    private final ExecutorService writer = Executors.newSingleThreadExecutor();
    private final Path file;
    private final Object ioLock = new Object();
    private volatile boolean closing;

    public TopicDataStore(Path file) {
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
            TopicData[] initialData = readFromFile();
            for (TopicData topicData : initialData) {
                data.put(topicData.topicName(), topicData);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read topic data for " + file, e);
        }

        writer.submit(this::processWrites);
    }

    public void save(TopicData topicData) {
        data.put(topicData.topicName(), topicData);
        if (!closing) {
            queue.offer(true);
        }
    }

    public TopicData[] getAll() {
        return data.values().toArray(new TopicData[0]);
    }

    public void delete(String topicName) {
        data.remove(topicName);
        if (!closing) {
            queue.offer(true);
        }
    }

    private void processWrites() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                queue.take();
                writeToFile(Map.copyOf(data));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to write topic data", e);
                if (!closing) {
                    queue.offer(true);
                }
            }
        }
    }

    private void writeToFile(Map<String, TopicData> data) throws IOException {
        synchronized (ioLock) {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writeValue(tmp.toFile(), data.values());
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private TopicData[] readFromFile() throws IOException {
        if (!Files.exists(file)) {
            return new TopicData[0];
        }

        return MAPPER.readValue(file.toFile(), TopicData[].class);
    }

    public void shutdown() {
        closing = true;
        writer.shutdownNow();

        try {
            if (!writer.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warning("Timed out waiting for topic data writer thread to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            writeToFile(Map.copyOf(data));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write topic data during shutdown", e);
        }
    }
}
 