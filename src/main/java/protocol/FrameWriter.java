package protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

public class FrameWriter {
    public static void writeFrame(DataOutputStream out, byte type, UUID id, byte[] payload) throws IOException {
        byte[] safePayload = payload == null ? new byte[0] : payload;

        out.writeByte(type);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeInt(safePayload.length);
        out.write(safePayload);
        out.flush();
    }
}
