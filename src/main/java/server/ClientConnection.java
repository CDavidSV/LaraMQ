package server;

import command.*;
import protocol.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientConnection implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ClientConnection.class.getName());
    private final UUID id = UUID.randomUUID();
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final CommandRegistry cmdRegistry;

    ClientConnection(Socket socket, CommandRegistry cmdRegistry) throws IOException {
        this.socket = socket;
        this.cmdRegistry = cmdRegistry;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(socket.getOutputStream());
    }

    public void reader() throws IOException, ProtocolException {
        while(true) {
            Frame frame = FrameReader.readFrame(in);

            try {
                CommandCode code = CommandCode.valueOf(frame.type());
                Command cmd = cmdRegistry.get(code);
                DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(frame.payload()));
                byte[] resData = cmd.execute(this, payloadIn);
                sendAck(frame.id(), resData);
            } catch (CommandException | CommandExecutionException | IOException e) {
                logger.log(Level.WARNING, e.getMessage());
                sendError(frame.id(), e.getMessage());
            }
        }
    }

    public void sendError(UUID id, String message) throws IOException {
        FrameWriter.writeFrame(out, MessageCode.ERROR.code, id, message.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized void sendAck(UUID id, byte[] data) throws IOException {
        FrameWriter.writeFrame(out, MessageCode.ACK.code, id, data);
    }

    public void sendMessage(String topic, byte[] payload) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);
        dos.writeUTF(topic);
        dos.write(payload);

        synchronized (out) {
            FrameWriter.writeFrame(out, MessageCode.NOTIFICATION.code, UUID.randomUUID(), buf.toByteArray());
        }
    }

    public UUID getId() {
        return id;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage());
        }
    }
}
