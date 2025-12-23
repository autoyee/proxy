package io.gateway.plugin;

import io.netty.handler.codec.http.FullHttpRequest;
import java.util.concurrent.*;

/**
 * @author yee
 */
public class PluginChainExecutor {
    public CompletableFuture<Void> executeRequest(FullHttpRequest req) {
        // demo: no-op plugin chain
        return CompletableFuture.completedFuture(null);
    }
}