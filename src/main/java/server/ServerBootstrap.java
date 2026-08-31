package server;

import broker.Broker;
import command.*;
import services.analytics.AnalyticsService;

import java.nio.file.Path;

public class ServerBootstrap {
    private static final Path DEFAULT_DIR = Path.of("data");
    public static final Path DEFAULT_TOPIC_DATA_FILE_PATH = DEFAULT_DIR.resolve("topic_data.json");
    public static final Path DEFAULT_CLIENT_SESSION_DATA_FILE_PATH = DEFAULT_DIR.resolve("client_sessions.json");

    public static CommandRegistry buildRegistry(Broker broker, AnalyticsService analyticsService) {
        CommandRegistry registry = new CommandRegistry();

        // Register commands here
        registry.register(CommandCode.SUBSCRIBE, new SubscribeCommand(broker, analyticsService));
        registry.register(CommandCode.UNSUBSCRIBE, new UnsubscribeCommand(broker, analyticsService));
        registry.register(CommandCode.PUBLISH, new PublishCommand(broker, analyticsService));
        registry.register(CommandCode.LIST, new ListCommand(broker));
        registry.register(CommandCode.ANALYTICS, new AnalyticsCommand(analyticsService));

        return registry;
    }
}
