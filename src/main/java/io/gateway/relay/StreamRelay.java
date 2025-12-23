package io.gateway.relay;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.util.HttpCopier;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yee
 */
public class StreamRelay {

    private final Channel upstream;
    private final Channel downstream;
    private final EventLoop eventLoop;
    private final String serviceKey;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public StreamRelay(Channel upstream, Channel downstream, EventLoop eventLoop, String serviceKey) {
        this.upstream = upstream;
        this.downstream = downstream;
        this.eventLoop = eventLoop;
        this.serviceKey = serviceKey;
        bindDownstream();
    }

    public void forwardUpstreamContent(HttpContent content) {
        if (closed.get()) {
            ReferenceCountUtil.release(content);
            return;
        }

        ByteBuf buf = content.content();
        if (buf.isReadable()) {
            downstream.write(new DefaultHttpContent(buf.retainedDuplicate()));
        }
        if (content instanceof LastHttpContent) {
            downstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        }
        ReferenceCountUtil.release(content);
    }

    private void bindDownstream() {
        downstream.pipeline().addLast("relay", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (closed.get()) {
                    ReferenceCountUtil.release(msg);
                    return;
                }
                try {
                    if (msg instanceof HttpResponse) {
                        HttpResponse resp = (HttpResponse) msg;
                        upstream.writeAndFlush(HttpCopier.copyResponse(resp));
                        return;
                    }
                    if (msg instanceof HttpContent) {
                        HttpContent content = (HttpContent) msg;
                        ByteBuf buf = content.content();
                        if (buf.isReadable()) {
                            upstream.write(new DefaultHttpContent(buf.retainedDuplicate()));
                        }
                        if (content instanceof LastHttpContent) {
                            upstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                        }
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        PerEventLoopChannelPoolManager.getInstance().release(eventLoop, serviceKey, downstream);

        if (upstream.isActive()) {
            upstream.close();
        }
        if (downstream.isActive()) {
            downstream.close();
        }
    }
}
