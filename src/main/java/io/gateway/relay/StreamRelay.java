package io.gateway.relay;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.util.HttpCopier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.gateway.common.Constants.HTTP_CODEC;
import static io.gateway.common.Constants.RELAY;

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

    // 标记本次 HTTP 交互是否完整成功
    private volatile boolean exchangeComplete = false;

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
            log.info("Forwarding httpContent to downstream, readable bytes: {}", buf.readableBytes());
            DefaultHttpContent msg = new DefaultHttpContent(buf.retainedDuplicate());
            downstream.write(msg)
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            log.info("Forwarding httpContent to downstream write success");
                        } else {
                            // 写失败直接关闭
                            close();
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
                            close();
                            log.error("Failed to write lastHttpContent to downstream", future.cause());
                        }
                    });
        }
    }

    private void bindDownstream() {
        ChannelPipeline pipeline = downstream.pipeline();
        if (pipeline.get(HTTP_CODEC) == null) {
            // 防御性关闭
            close();
            throw new IllegalStateException("HttpClientCodec missing in downstream pipeline");
        }

        pipeline.addLast(RELAY, new ChannelInboundHandlerAdapter() {
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
                        log.info("Received httpResponse from downstream, status: {}", resp.status());
                        // 收到响应头，开始转发
                        upstream.writeAndFlush(HttpCopier.copyResponse(resp))
                                .addListener(future -> {
                                    if (future.isSuccess()) {
                                        log.info("Successfully forwarded httpResponse to upstream");
                                    } else {
                                        close();
                                        log.error("Failed to write httpResponse to upstream", future.cause());
                                    }
                                });
                    } else if (msg instanceof HttpContent) {
                        HttpContent content = (HttpContent) msg;
                        ByteBuf buf = content.content();

                        if (buf.isReadable()) {
                            log.info("Received httpContent from downstream, readable bytes: {}", buf.readableBytes());
                            upstream.write(new DefaultHttpContent(buf.retainedDuplicate()))
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Successfully forwarded httpContent to upstream");
                                        } else {
                                            log.error("Failed to forward httpContent to upstream", future.cause());
                                        }
                                    });
                        }

                        if (content instanceof LastHttpContent) {
                            log.info("Received lastHttpContent from downstream");
                            // 收到 LastHttpContent，且无异常，标记为交换完成
                            exchangeComplete = true;
                            upstream.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Successfully forwarded lastHttpContent to upstream");
                                            // 交互完美结束，清理 Handler 并归还连接池
                                            ctx.pipeline().remove(this);
                                            releaseDownstream();
                                        } else {
                                            close();
                                            log.error("Failed to forward lastHttpContent to upstream", future.cause());
                                        }
                                    });
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception processing message from downstream for serviceKey: {}", serviceKey, e);
                    close();
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                log.info("Downstream channel inactive, closing streamRelay for serviceKey: {}", serviceKey);
                // 如果连接断开时交互未完成，说明是异常断开
                if (!exchangeComplete) {
                    close();
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                log.error("Exception in downstream handler for serviceKey: {}, cause: {}", serviceKey, cause.getMessage());
                close();
            }
        });
    }

    /**
     * 正常归还连接
     */
    private void releaseDownstream() {
        if (closed.compareAndSet(false, true)) {
            PerEventLoopChannelPoolManager.getInstance().release(eventLoop, serviceKey, downstream);
        }
    }

    /**
     * 异常关闭（销毁连接）
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Force closing relay for {}", serviceKey);

        // 只有 exchangeComplete 为 true 时才归还池，否则直接 close downstream
        // 这里因为是异常调用 close()，说明状态肯定不好，直接 destroy
        if (downstream != null) {
            // 这里的 close 会触发 FixedChannelPool 的清理逻辑
            downstream.close();
        }

        if (upstream != null && upstream.isActive()) {
            upstream.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
