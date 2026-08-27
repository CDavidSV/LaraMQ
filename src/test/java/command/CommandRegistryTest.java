package command;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CommandRegistryTest {

    @Test
    void registerAndGetReturnsSameCommandInstance() {
        CommandRegistry registry = new CommandRegistry();
        Command command = mock(Command.class);

        registry.register(CommandCode.LIST, command);

        assertSame(command, registry.get(CommandCode.LIST));
    }

    @Test
    void getThrowsWhenCommandNotRegistered() {
        CommandRegistry registry = new CommandRegistry();

        assertThrows(CommandException.class, () -> registry.get(CommandCode.ANALYTICS));
    }
}

