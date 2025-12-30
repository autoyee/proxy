package io.gateway.relay;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.client.PoolKey;
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
import lombok.extern.slf4j.Slf4j;

/**
 * @author yee
 */
@Slf4j
public class StreamRelay {

    private final Channel upstream;
    private final Channel downstream;
    private final EventLoop eventLoop;
    private final PoolKey poolKey;
    private volatile RelayState state = RelayState.INIT;

    public StreamRelay(Channel upstream, Channel downstream, EventLoop eventLoop, PoolKey poolKey) {
        this.upstream = upstream;
        this.downstream = downstream;
        this.eventLoop = eventLoop;
        this.poolKey = poolKey;

        bindDownstream();
    }

    /* ==================== upstream -> downstream ==================== */
    public void forwardRequest(HttpContent content) {
        if (state != RelayState.INIT && state != RelayState.STREAMING) {
            ReferenceCountUtil.release(content);
            return;
        }

        state = RelayState.STREAMING;
        downstream.eventLoop().execute(() -> {
            ByteBuf buf = content.content();
            if (buf.isReadable()) {
                log.debug("Forwarding httpContent to downstream, readable bytes: {}", buf.readableBytes());
                DefaultHttpContent msg = new DefaultHttpContent(buf.retainedDuplicate());
                downstream.write(msg)
                        .addListener(future -> {
                            if (future.isSuccess()) {
                                log.debug("Forwarding httpContent to downstream write success");
                            } else {
                                fail(future.cause());
                                log.error("Failed to write httpContent to downstream", future.cause());
                            }
                        });
            }

            if (content instanceof LastHttpContent) {
                log.info("Forwarding lastHttpContent to downstream");
                downstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                        .addListener(future -> {
                            if (future.isSuccess()) {
                                log.info("Forwarding lastHttpContent to downstream success");
                            } else {
                                fail(future.cause());
                                log.error("Failed to write lastHttpContent to downstream", future.cause());
                            }
                        });
            }

            ReferenceCountUtil.release(content);
        });
    }

    /* ==================== downstream -> upstream ==================== */
    private void bindDownstream() {
        downstream.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                try {
                    if (state == RelayState.FAILED || state == RelayState.CLOSED) {
                        ReferenceCountUtil.release(msg);
                        return;
                    }

                    if (msg instanceof HttpResponse) {
                        HttpResponse resp = (HttpResponse) msg;
                        log.info("Received httpResponse from downstream, status: {}", resp.status());
                        // 收到响应头，开始转发
                        upstream.writeAndFlush(HttpCopier.copyResponse(resp))
                                .addListener(future -> {
                                    if (future.isSuccess()) {
                                        log.info("Successfully forwarded httpResponse to upstream");
                                    } else {
                                        fail(future.cause());
                                        log.error("Failed to write httpResponse to upstream", future.cause());
                                    }
                                });
                    } else if (msg instanceof HttpContent) {
                        HttpContent content = (HttpContent) msg;
                        ByteBuf buf = content.content();

                        if (buf.isReadable()) {
                            log.debug("Received httpContent from downstream, readable bytes: {}", buf.readableBytes());
                            upstream.write(new DefaultHttpContent(buf.retainedDuplicate()))
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.debug("Successfully forwarded httpContent to upstream");
                                        } else {
                                            fail(future.cause());
                                            log.error("Failed to forward httpContent to upstream", future.cause());
                                        }
                                    });
                        }

                        if (content instanceof LastHttpContent) {
                            log.info("Received lastHttpContent from downstream");
                            state = RelayState.COMPLETED;
                            upstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Successfully forwarded lastHttpContent to upstream");
                                            // 交互完美结束，清理 Handler 并归还连接池
                                            ctx.pipeline().remove(this);
                                            release();
                                        } else {
                                            fail(future.cause());
                                            log.error("Failed to forward lastHttpContent to upstream", future.cause());
                                        }
                                    });
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception processing message from downstream for serviceKey: {}", poolKey.scheme(), e);
                    fail(e);
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                fail(cause);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                if (state != RelayState.COMPLETED) {
                    fail(null);
                }
            }
        });
    }

    /* ==================== lifecycle ==================== */
    private void release() {
        if (state != RelayState.COMPLETED) return;
        state = RelayState.CLOSED;
        PerEventLoopChannelPoolManager.getInstance().release(eventLoop, poolKey, downstream);
    }

    private void fail(Throwable t) {
        if (state == RelayState.CLOSED) return;
        state = RelayState.FAILED;

        downstream.close();
        if (upstream.isActive()) {
            upstream.close();
        }
    }

    /**
     * 主动关闭（上游取消 / context 清理 / server shutdown）
     * 语义：等同于失败路径，永不归还连接池
     */
    public void close() {
        if (state == RelayState.CLOSED) return;
        state = RelayState.CLOSED;

        try {
            if (downstream != null) {
                downstream.close();
            }
        } finally {
            if (upstream != null && upstream.isActive()) {
                upstream.close();
            }
        }
    }
}
