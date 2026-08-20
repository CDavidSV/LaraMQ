package server;

import broker.Broker;
import command.*;

public class ServerBootstrap {
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
