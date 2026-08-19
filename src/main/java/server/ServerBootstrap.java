package server;

import command.CommandCode;
import command.CommandRegistry;
import command.SubscribeCommand;

public class ServerBootstrap {
    public static CommandRegistry buildRegistry() {
        CommandRegistry registry = new CommandRegistry();

        // Register commands here
        registry.register(CommandCode.SUBSCRIBE, new SubscribeCommand());

        return registry;
    };
}
