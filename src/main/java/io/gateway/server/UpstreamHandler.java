package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.client.PoolKey;
import io.gateway.config.GatewayConfig;
import io.gateway.relay.ProductionStreamRelay;
import io.gateway.util.HttpCopier;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yee
 */
@Slf4j
public final class UpstreamHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        EventLoop el = ctx.channel().eventLoop();
        PoolKey key = new PoolKey("http", GatewayConfig.BACKEND_HOST, GatewayConfig.BACKEND_PORT, "http1");
        HttpRequest copied = HttpCopier.copyFullRequest(req);

        PerEventLoopChannelPoolManager.getInstance()
                .acquire(el, key)
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        log.info("1");
                        sendError(ctx, HttpResponseStatus.BAD_GATEWAY);
                        return;
                    }

                    Channel downstream = (Channel) f.getNow();
                    downstream.eventLoop().execute(() -> {
                        if (!downstream.isActive()) {
                            PerEventLoopChannelPoolManager.getInstance().destroy(el, key, downstream);
                            log.info("2");
                            sendError(ctx, HttpResponseStatus.BAD_GATEWAY);
                            return;
                        }

                        ProductionStreamRelay relay = new ProductionStreamRelay(ctx.channel(), downstream, el, key);
                        downstream.writeAndFlush(copied)
                                .addListener(wf -> {
                                    if (!wf.isSuccess()) {
                                        relay.close();
                                        log.info("3");
                                        sendError(ctx, HttpResponseStatus.BAD_GATEWAY);
                                    }
                                });
                    });
                });
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }
}
