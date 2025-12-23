package io.gateway.relay;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yee
 */
@Slf4j
public class StreamRelay {
    private final Channel upstream;
    private final Channel downstream;

    public StreamRelay(Channel upstream, Channel downstream) {
        this.upstream = upstream;
        this.downstream = downstream;
        log.info("StreamRelay initialized with upstream: {} and downstream: {}", upstream, downstream);

        attach();
    }

    private void attach() {
        downstream.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpResponse || msg instanceof HttpContent) {
                    ReferenceCountUtil.retain(msg);
                    upstream.writeAndFlush(msg);
                }
            }
        });
    }

    public void start(FullHttpRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("Request cannot be null");
        }
        if (downstream == null) {
            throw new IllegalStateException("Downstream handler is not initialized");
        }

        try {
            downstream.writeAndFlush(req.retain()).addListener(future -> {
                if (!future.isSuccess()) {
                    // 异常处理：释放资源
                    req.release();
                }
            });
        } catch (Exception e) {
            // 异常处理：释放资源
            req.release();
            throw e;
        }
    }

}
