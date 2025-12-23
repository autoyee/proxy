package io.gateway.relay;

/**
 * @author yee
 */
public class StreamRequestContext {
    private final StreamRelay relay;

    public StreamRequestContext(StreamRelay relay) {
        this.relay = relay;
    }

    public StreamRelay getRelay() {
        return relay;
    }
}