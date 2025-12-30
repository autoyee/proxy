package io.gateway.relay;

import io.gateway.client.PoolKey;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;

public final class ProductionStreamRelay {

    private final Channel upstream;
    private final Channel downstream;
    private final EventLoop el;
    private final PoolKey key;

    public ProductionStreamRelay(Channel upstream, Channel downstream, EventLoop el, PoolKey key) {
        this.upstream = upstream;
        this.downstream = downstream;
        this.el = el;
        this.key = key;
        bind();
    }

    private void bind() {
        downstream.pipeline().addLast(new ChannelInboundHandlerAdapter() {

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                upstream.writeAndFlush(msg);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                close();
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                close();
            }
        });
    }

    public void close() {
        downstream.close();
        if (upstream.isActive()) {
            upstream.close();
        }
    }
}
