package io.github.ariellaub.protobus;

/**
 * No loaded service matches a proxy's name, or any prefix of it.
 */
public class InvalidServiceNameError extends ProtobusException {
    private static final long serialVersionUID = 1L;

    public InvalidServiceNameError() {
        super(null, null);
    }

    public InvalidServiceNameError(String message) {
        super(message, null);
    }
}
