package io.gateway.client;

import lombok.Data;

/**
 * @author yee
 */
@Data
public class PoolKey {
    private final String scheme;
    private final String host;
    private final int port;
    private final String protocol;

    public PoolKey(String scheme, String host, int port, String protocol) {
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.protocol = protocol;
    }

    public String scheme() {
        return scheme;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String protocol() {
        return protocol;
    }
}
