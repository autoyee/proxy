package io.gateway.relay;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.util.HttpCopier;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yee
 */
@Slf4j
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
            log.info("StreamRelay is closed, releasing content");
            ReferenceCountUtil.release(content);
            return;
        }

        ByteBuf buf = content.content();
        if (buf.isReadable()) {
            log.info("Forwarding content to downstream, readable bytes: {}", buf.readableBytes());
            HttpContent newContent = new DefaultHttpContent(buf.retainedDuplicate());
            downstream.write(newContent)
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            log.info("Forwarding content to downstream write success");
                        } else {
                            log.error("Failed to write content to downstream", future.cause());
                        }
                    });
        }
        if (content instanceof LastHttpContent) {
            log.info("Forwarding LastHttpContent to downstream");
            downstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            log.info("Forwarding LastHttpContent to downstream success");
                        } else {
                            log.error("Failed to write LastHttpContent to downstream", future.cause());
                        }
                    });
        }
    }

    private void bindDownstream() {
        ChannelPipeline pipeline = downstream.pipeline();

        if (pipeline.get("httpCodec") == null) {
            throw new IllegalStateException("HttpClientCodec missing in downstream pipeline");
        }

        // relay 必须在 codec 后面（inbound）
        pipeline.addLast("relay", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (closed.get()) {
                    log.info("StreamRelay is closed, releasing message from downstream");
                    ReferenceCountUtil.release(msg);
                    return;
                }
                try {
                    if (msg instanceof HttpResponse) {
                        HttpResponse resp = (HttpResponse) msg;
                        log.info("Received HttpResponse from downstream, status: {}", resp.status());
                        upstream.writeAndFlush(HttpCopier.copyResponse(resp))
                                .addListener(future -> {
                                    if (future.isSuccess()) {
                                        log.info("Successfully forwarded HttpResponse to upstream");
                                    } else {
                                        log.error("Failed to write HttpResponse to upstream", future.cause());
                                    }
                                });
                    } else if (msg instanceof HttpContent) {
                        HttpContent content = (HttpContent) msg;
                        ByteBuf buf = content.content();
                        if (buf.isReadable()) {
                            log.info("Received HttpContent from downstream, readable bytes: {}", buf.readableBytes());
                            upstream.write(new DefaultHttpContent(buf.retainedDuplicate()))
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Successfully forwarded HttpContent to upstream");
                                        } else {
                                            log.error("Failed to forward HttpContent to upstream", future.cause());
                                        }
                                    });
                        }
                        if (content instanceof LastHttpContent) {
                            log.info("Received LastHttpContent from downstream");
                            upstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Successfully forwarded LastHttpContent to upstream");
                                        } else {
                                            log.error("Failed to forward LastHttpContent to upstream", future.cause());
                                        }
                                    });
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception processing message from downstream for serviceKey: {}", serviceKey, e);
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                log.info("Downstream channel inactive, closing StreamRelay for serviceKey: {}", serviceKey);
                close();
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                log.error("Exception in downstream handler for serviceKey: {}, cause: {}", serviceKey, cause.getMessage());
                close();
            }
        });
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            log.info("StreamRelay already closed for serviceKey: {}", serviceKey);
            return;
        }

        log.info("Closing StreamRelay for serviceKey: {}, releasing downstream channel", serviceKey);
        PerEventLoopChannelPoolManager.getInstance().release(eventLoop, serviceKey, downstream);

        if (upstream.isActive()) {
            log.info("Closing upstream channel: {}", upstream.id());
            upstream.close();
        }
        if (downstream.isActive()) {
            log.info("Closing downstream channel: {}", downstream.id());
            downstream.close();
        }
    }
}
