package command;

import server.ClientConnection;

import java.io.DataInputStream;
import java.io.IOException;

public abstract class Command {
    public abstract byte[] execute(ClientConnection conn, DataInputStream in) throws IOException, CommandExecutionException;
}
