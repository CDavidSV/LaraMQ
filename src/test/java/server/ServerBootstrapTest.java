package server;

import broker.Broker;
import broker.store.TopicDataStore;
import command.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import services.analytics.AnalyticsService;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ServerBootstrapTest {

    @TempDir
    Path tempDir;

    private TopicDataStore topicDataStore;
    private ClientSessionHandler clientSessionHandler;
    private Broker broker;

    @BeforeEach
    void setUp() {
        topicDataStore = new TopicDataStore(tempDir.resolve("topic_data.json"));
        clientSessionHandler = new ClientSessionHandler(tempDir.resolve("client_sessions.json"));
        broker = new Broker(topicDataStore, new AnalyticsService(), clientSessionHandler);
    }

    @AfterEach
    void tearDown() {
        broker.shutdown();
        clientSessionHandler.close();
    }

    @Test
    void buildRegistryRegistersAllExpectedCommandHandlers() {
        AnalyticsService analyticsService = new AnalyticsService();
        CommandRegistry registry = ServerBootstrap.buildRegistry(broker, analyticsService);

        assertInstanceOf(SubscribeCommand.class, registry.get(CommandCode.SUBSCRIBE));
        assertInstanceOf(UnsubscribeCommand.class, registry.get(CommandCode.UNSUBSCRIBE));
        assertInstanceOf(PublishCommand.class, registry.get(CommandCode.PUBLISH));
        assertInstanceOf(ListCommand.class, registry.get(CommandCode.LIST));
        assertInstanceOf(AnalyticsCommand.class, registry.get(CommandCode.ANALYTICS));
    }
}

