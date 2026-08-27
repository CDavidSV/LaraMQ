package command;

import broker.Broker;
import org.junit.jupiter.api.Test;
import server.ClientConnection;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ListCommandTest {

    @Test
    void executeReturnsCommaSeparatedTopicList() {
        Broker broker = mock(Broker.class);
        when(broker.listTopics()).thenReturn(new String[]{"weather", "news"});

        ListCommand command = new ListCommand(broker);
        ClientConnection connection = mock(ClientConnection.class);

        byte[] response = command.execute(connection, new DataInputStream(new ByteArrayInputStream(new byte[0])));

        assertEquals("weather, news", new String(response, StandardCharsets.UTF_8));
    }
}

