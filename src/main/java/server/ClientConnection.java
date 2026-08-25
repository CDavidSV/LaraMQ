package server;

import command.*;
import protocol.*;
import services.analytics.AnalyticsService;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientConnection implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ClientConnection.class.getName());
    private static final int OUTBOUND_QUEUE_CAPACITY = 4096;
    private final UUID id;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final CommandRegistry cmdRegistry;
    private final AnalyticsService analyticsService;
    private final BlockingQueue<OutboundFrame> outboundQueue = new ArrayBlockingQueue<>(OUTBOUND_QUEUE_CAPACITY);
    private final Thread outboundWriter;
    private volatile boolean closed;

    ClientConnection(Socket socket, DataInputStream in, CommandRegistry cmdRegistry, AnalyticsService analyticsService, UUID id) throws IOException, AuthenticationException, ProtocolException {
        this.socket = socket;
        this.cmdRegistry = cmdRegistry;
        this.analyticsService = analyticsService;
        this.in = in;
        this.id = id;
        out = new DataOutputStream(socket.getOutputStream());

        outboundWriter = Thread.startVirtualThread(this::runOutboundWriter);
    }

    private record OutboundFrame(byte type, UUID id, byte[] payload) {
    }

    private void runOutboundWriter() {
        while (!closed) {
            try {
                OutboundFrame frame = outboundQueue.take();
                synchronized (out) {
                    FrameWriter.writeFrame(out, frame.type(), frame.id(), frame.payload());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.log(Level.WARNING, "failed to write outbound frame for client (ID: %s): %s".formatted(id, e.getMessage()));
                closed = true;
                break;
            }
        }
    }

    private void enqueueOutbound(OutboundFrame frame) throws IOException {
        if (closed) {
            throw new IOException("connection is closed");
        }

        boolean enqueued = outboundQueue.offer(frame);
        if (!enqueued) {
            throw new IOException("outbound queue is full");
        }
    }

    public void reader() throws IOException, ProtocolException {
        while (true) {
            Frame frame = FrameReader.readFrame(in);

            if (frame.type() != MessageCode.COMMAND.code) {
                throw new ProtocolException("Expected COMMAND frame, got message code: " + frame.type());
            }

            try {
                DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(frame.payload()));
                byte commandByte = payloadIn.readByte();
                CommandCode code = CommandCode.valueOf(commandByte);
                Command cmd = cmdRegistry.get(code);
                analyticsService.recordCommand(code);
                byte[] resData = cmd.execute(this, payloadIn);
                sendAck(frame.id(), resData);
            } catch (CommandException | CommandExecutionException | IOException e) {
                logger.log(Level.WARNING, e.getMessage());
                sendError(frame.id(), e.getMessage());
            }
        }
    }

    public void sendError(UUID id, String message) throws IOException {
        byte[] payload = message == null ? new byte[0] : message.getBytes(StandardCharsets.UTF_8);
        enqueueOutbound(new OutboundFrame(MessageCode.ERROR.code, id, payload));
    }

    public void sendAck(UUID id, byte[] data) throws IOException {
        byte[] payload = data == null ? new byte[0] : data.clone();
        enqueueOutbound(new OutboundFrame(MessageCode.ACK.code, id, payload));
    }

    public void sendMessage(String topic, byte[] payload) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf);
        dos.writeUTF(topic);
        dos.write(payload);

        enqueueOutbound(new OutboundFrame(MessageCode.NOTIFICATION.code, UUID.randomUUID(), buf.toByteArray()));
    }

    public UUID getId() {
        return id;
    }

    @Override
    public void close() {
        closed = true;
        outboundWriter.interrupt();
        try {
            socket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage());
        }
    }
}
