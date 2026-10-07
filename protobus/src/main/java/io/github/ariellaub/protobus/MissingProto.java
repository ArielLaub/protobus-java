package io.github.ariellaub.protobus;

/**
 * No schema declares the service a class serves.
 */
public class MissingProto extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public MissingProto() {
        super(null, null);
    }

    public MissingProto(String message) {
        super(message, null);
    }
}
