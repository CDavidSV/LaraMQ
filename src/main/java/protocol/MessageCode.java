package protocol;

public enum MessageCode {
    ACK((byte) 0x01),
    ERROR((byte) 0x02),
    NOTIFICATION((byte) 0x03),
    AUTHENTICATE((byte) 0x04),
    COMMAND((byte) 0x05);

    public final byte code;

    MessageCode(byte code) {
        this.code = code;
    }
}
