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

public class FileTopicDataStore extends TopicDataStore {
    private static final Logger logger = Logger.getLogger(FileTopicDataStore.class.getName());
    private final Map<String, TopicData> data = new ConcurrentHashMap<>();
    private final BlockingQueue<Boolean> queue = new LinkedBlockingQueue<>(1);
    private final ExecutorService writer = Executors.newSingleThreadExecutor();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Path file;

    public FileTopicDataStore(Path file) {
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

    @Override
    public void save(TopicData topicData) {
        data.put(topicData.topicName(), topicData);

        queue.offer(true);
    }

    @Override
    public TopicData[] getAll() {
        return data.values().toArray(new TopicData[0]);
    }

    @Override
    public void delete(String topicName) {
        data.remove(topicName);

        queue.offer(true);
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
                queue.offer(true);
            }
        }
    }

    private void writeToFile(Map<String, TopicData> data) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        MAPPER.writeValue(tmp.toFile(), data.values());
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private TopicData[] readFromFile() throws IOException {
        if (!Files.exists(file)) {
            return new TopicData[0];
        }

        return MAPPER.readValue(file.toFile(), TopicData[].class);
    }

    public void shutdown() {
        try {
            writeToFile(data);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write topic data during shutdown", e);
        }
        writer.shutdownNow();
    }
}
 