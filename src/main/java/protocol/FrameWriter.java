package protocol;

import java.awt.*;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

public class FrameWriter {
    public static void writeFrame(DataOutputStream out, byte type, UUID id, byte[] payload) throws IOException {
        out.writeByte(type);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }
}
