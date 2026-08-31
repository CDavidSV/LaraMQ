package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ClientSessionHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void getOrCreateAndClosePersistsSessionData() {
        Path file = tempDir.resolve("client_sessions.json");
        UUID clientId = UUID.randomUUID();

        ClientSessionHandler writer = new ClientSessionHandler(file);
        ClientSession session = writer.getOrCreate(clientId);
        session.subscribeTopic("weather");
        session.enqueueUndeliveredMessage("weather", "queued".getBytes());
        writer.close();

        ClientSessionHandler reader = new ClientSessionHandler(file);
        try {
            ClientSession loadedSession = reader.get(clientId.toString());
            assertNotNull(loadedSession);

            ClientSessionData data = loadedSession.toData();
            assertTrue(Arrays.asList(data.subscribedTopics()).contains("weather"));
            assertArrayEquals("queued".getBytes(), data.undeliveredMessages().get("weather")[0]);
        } finally {
            reader.close();
        }
    }

    @Test
    void removeDeletesSessionFromInMemoryState() {
        Path file = tempDir.resolve("client_sessions.json");
        UUID clientId = UUID.randomUUID();

        ClientSessionHandler handler = new ClientSessionHandler(file);
        try {
            handler.getOrCreate(clientId);
            handler.remove(clientId.toString());

            assertNull(handler.get(clientId.toString()));
        } finally {
            handler.close();
        }
    }
}

