package command;

import server.ClientConnection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class Command {
    public abstract void execute(ClientConnection conn, DataInputStream in, DataOutputStream out) throws IOException;
}
