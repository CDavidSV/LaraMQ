package command;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandCodeTest {

    @Test
    void valueOfReturnsExpectedCommandCode() {
        assertEquals(CommandCode.SUBSCRIBE, CommandCode.valueOf((byte) 0x01));
        assertEquals(CommandCode.UNSUBSCRIBE, CommandCode.valueOf((byte) 0x02));
        assertEquals(CommandCode.PUBLISH, CommandCode.valueOf((byte) 0x03));
        assertEquals(CommandCode.LIST, CommandCode.valueOf((byte) 0x04));
        assertEquals(CommandCode.ANALYTICS, CommandCode.valueOf((byte) 0x05));
    }

    @Test
    void valueOfThrowsForUnknownCode() {
        assertThrows(CommandException.class, () -> CommandCode.valueOf((byte) 0x7F));
    }
}

