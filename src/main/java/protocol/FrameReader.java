package protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.UUID;

public class FrameReader {
    private static final int MAX_PAYLOAD_SIZE = 10 * 1024 * 1024;

    public static Frame readFrame(DataInputStream in) throws IOException, ProtocolException {
        byte type = in.readByte();
        long msb = in.readLong();
        long lsb = in.readLong();
        UUID id = new UUID(msb,lsb);

        int length = in.readInt();
        if (length < 0 || length > MAX_PAYLOAD_SIZE) {
            throw new ProtocolException("Invalid payload length: " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new Frame(type, id, payload);
    }
}
