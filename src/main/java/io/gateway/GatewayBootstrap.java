package io.gateway;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.plugin.PluginChainExecutor;
import io.gateway.server.GatewayServer;

/**
 * @author yee
 */
public class GatewayBootstrap {
    public static void main(String[] args) throws Exception {
        GatewayServer server = new GatewayServer(8);
        server.start(8080, new PluginChainExecutor());
    }
}