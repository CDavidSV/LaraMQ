package protocol;

import java.util.UUID;

public record Frame(byte type, UUID id, byte[] payload) {
    @Override
    public String toString() {
        return "--- New Message ---\nType: %s\nId: %s\nPayload: %s".formatted(type, id, payload);
    }
}
