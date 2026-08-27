package command;

import java.util.HashMap;
import java.util.Map;

public enum CommandCode {
    SUBSCRIBE((byte) 0x01),
    UNSUBSCRIBE((byte) 0x02),
    PUBLISH((byte) 0x03),
    LIST((byte) 0x04),
    ANALYTICS((byte) 0x05);

    private static final Map<Byte, CommandCode> BY_CODE = new HashMap<>();

    static {
        for (CommandCode c : values()) BY_CODE.put(c.code, c);
    }

    public final byte code;

    CommandCode(byte code) {
        this.code = code;
    }

    public static CommandCode valueOf(byte code) {
        CommandCode c = BY_CODE.get(code);
        if (c == null) {
            throw new CommandException("Unknown command code: " + code);
        }
        return c;
    }
}
