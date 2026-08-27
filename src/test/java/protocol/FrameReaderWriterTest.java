package protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FrameReaderWriterTest {

    @Test
    void roundTripFramePreservesTypeIdAndPayload() throws IOException, ProtocolException {
        UUID id = UUID.randomUUID();
        byte[] payload = new byte[]{1, 2, 3, 4, 5};

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);
        FrameWriter.writeFrame(dataOutput, MessageCode.COMMAND.code, id, payload);

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
        Frame frame = FrameReader.readFrame(dataInput);

        assertEquals(MessageCode.COMMAND.code, frame.type());
        assertEquals(id, frame.id());
        assertArrayEquals(payload, frame.payload());
    }

    @Test
    void writeFrameWithNullPayloadRoundTripsAsEmptyPayload() throws IOException, ProtocolException {
        UUID id = UUID.randomUUID();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);
        FrameWriter.writeFrame(dataOutput, MessageCode.ACK.code, id, null);

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
        Frame frame = FrameReader.readFrame(dataInput);

        assertEquals(MessageCode.ACK.code, frame.type());
        assertEquals(id, frame.id());
        assertArrayEquals(new byte[0], frame.payload());
    }

    @Test
    void readFrameRejectsNegativePayloadLength() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);
        writeHeader(dataOutput, MessageCode.COMMAND.code, UUID.randomUUID());
        dataOutput.writeInt(-1);

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));

        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameReader.readFrame(dataInput));
        assertEquals("Invalid payload length: -1", exception.getMessage());
    }

    @Test
    void readFrameRejectsPayloadLargerThanMaxAllowed() throws IOException {
        int maxPayloadSize = 10 * 1024 * 1024;
        int oversizedLength = maxPayloadSize + 1;

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);
        writeHeader(dataOutput, MessageCode.COMMAND.code, UUID.randomUUID());
        dataOutput.writeInt(oversizedLength);

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));

        ProtocolException exception = assertThrows(ProtocolException.class, () -> FrameReader.readFrame(dataInput));
        assertEquals("Invalid payload length: " + oversizedLength, exception.getMessage());
    }

    @Test
    void readFrameThrowsWhenPayloadIsTruncated() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);
        writeHeader(dataOutput, MessageCode.COMMAND.code, UUID.randomUUID());
        dataOutput.writeInt(4);
        dataOutput.write(new byte[]{9, 8});

        DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(output.toByteArray()));

        assertThrows(EOFException.class, () -> FrameReader.readFrame(dataInput));
    }

    private static void writeHeader(DataOutputStream out, byte type, UUID id) throws IOException {
        out.writeByte(type);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }
}

