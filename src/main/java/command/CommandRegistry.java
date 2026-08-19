package command;

import java.util.HashMap;
import java.util.Map;

public class CommandRegistry {
    private final Map<CommandCode, Command> commands = new HashMap<>();

    public void register(CommandCode commandCode, Command command) {
        commands.put(commandCode, command);
    }

    public Command get(CommandCode code) {
        Command cmd = commands.get(code);
        if (cmd == null) {
            throw new CommandException("No handler registered for " + code);
        }
        return cmd;
    }
}
