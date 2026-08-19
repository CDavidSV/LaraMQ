package protocol;

import java.util.UUID;

public record Frame(byte type, UUID id, byte[] payload) {
    @Override
    public String toString() {
        return "Type: %s\nId: %s\nPayload: %s".formatted(type, id, payload);
    }
}
