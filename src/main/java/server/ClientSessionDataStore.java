package server;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientSessionDataStore {
	private static final Logger logger = Logger.getLogger(ClientSessionDataStore.class.getName());
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final Map<String, ClientSessionData> data = new ConcurrentHashMap<>();
	private final BlockingQueue<Boolean> queue = new LinkedBlockingQueue<>(1);
	private final ExecutorService writer = Executors.newSingleThreadExecutor();
	private final Path file;

	public ClientSessionDataStore(Path file) {
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
			for (ClientSessionData clientSessionData : initialData) {
				data.put(clientSessionData.clientId().toString(), clientSessionData);
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to read client session data for " + file, e);
		}

		writer.submit(this::processWrites);
	}

	public void save(ClientSessionData clientSessionData) {
		data.put(clientSessionData.clientId().toString(), clientSessionData);
		queue.offer(true);
	}

	public ClientSessionData[] getAll() {
		return data.values().toArray(new ClientSessionData[0]);
	}

	public void delete(String clientId) {
		data.remove(clientId);
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
				logger.log(Level.SEVERE, "Failed to write client session data", e);
				queue.offer(true);
			}
		}
	}

	private void writeToFile(Map<String, ClientSessionData> data) throws IOException {
		Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
		MAPPER.writeValue(tmp.toFile(), data.values());
		Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
	}

	private ClientSessionData[] readFromFile() throws IOException {
		if (!Files.exists(file)) {
			return new ClientSessionData[0];
		}

		return MAPPER.readValue(file.toFile(), ClientSessionData[].class);
	}

	public void shutdown() {
		try {
			writeToFile(Map.copyOf(data));
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to write client session data during shutdown", e);
		}
		writer.shutdownNow();
	}
}

