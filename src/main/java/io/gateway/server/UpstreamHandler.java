package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.plugin.PluginChainExecutor;
import io.gateway.relay.StreamRelay;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

/**
 * @author yee
 */
public class UpstreamHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final PerEventLoopChannelPoolManager poolManager;
    private final PluginChainExecutor plugins;

    public UpstreamHandler(PerEventLoopChannelPoolManager poolManager, PluginChainExecutor plugins) {
        this.poolManager = poolManager;
        this.plugins = plugins;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        plugins.executeRequest(req).thenAccept(v -> {
            String host = "127.0.0.1"; // demo downstream
            int port = 8081;
            poolManager.acquire(ctx.channel().eventLoop(), host, port)
                    .whenComplete((downstream, th) -> {
                        if (th != null) {
                            sendError(ctx);
                            return;
                        }
                        new StreamRelay(ctx.channel(), downstream).start(req);
                    });
        });
    }

    private void sendError(ChannelHandlerContext ctx) {
        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }
}