package server;

import broker.Broker;
import command.*;

import java.nio.file.Path;

public class ServerBootstrap {
    private static final Path DEFAULT_DIR = Path.of("data");
    public static final Path DEFAULT_TOPIC_DATA_FILE_PATH = DEFAULT_DIR.resolve("topic_data.json");

    public static CommandRegistry buildRegistry(Broker broker) {
        CommandRegistry registry = new CommandRegistry();

        // Register commands here
        registry.register(CommandCode.SUBSCRIBE, new SubscribeCommand(broker));
        registry.register(CommandCode.UNSUBSCRIBE, new UnsubscribeCommand(broker));
        registry.register(CommandCode.PUBLISH, new PublishCommand(broker));
        registry.register(CommandCode.LIST, new ListCommand(broker));

        return registry;
    }
}
