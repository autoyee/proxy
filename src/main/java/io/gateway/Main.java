package io.gateway;

import io.gateway.server.GatewayServer;
import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.plugin.PluginChainExecutor;

/**
 * @author yee
 */
public class Main {
    public static void main(String[] args) throws Exception {
        GatewayServer server = new GatewayServer(8);
        server.start(8080, new PerEventLoopChannelPoolManager(), new PluginChainExecutor());
    }
}