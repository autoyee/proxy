package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.client.PoolKey;
import io.gateway.plugin.PluginChainExecutor;
import io.gateway.relay.StreamRelay;
import io.gateway.relay.StreamRequestContext;
import io.gateway.util.HttpCopier;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author yee
 */
@Slf4j
public class UpstreamHandler extends ChannelInboundHandlerAdapter {

    private final PluginChainExecutor plugins;
    private static final AttributeKey<Deque<StreamRequestContext>> REQ_QUEUE = AttributeKey.valueOf("req_queue");
    private static final int MAX_PENDING_CHUNKS = 1024;

    private static final String UPSTREAM_HOST = "127.0.0.1";
    private static final int UPSTREAM_PORT = 8081;

    public UpstreamHandler(PluginChainExecutor plugins) {
        this.plugins = plugins;
    }

    /* ================= lifecycle ================= */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        ctx.channel().attr(REQ_QUEUE).set(new ArrayDeque<>());
    }

    /* ================= channelRead ================= */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Deque<StreamRequestContext> queue = ctx.channel().attr(REQ_QUEUE).get();

        try {
            if (msg instanceof HttpRequest) {
                handleHttpRequest(ctx, (HttpRequest) msg, queue);
            } else if (msg instanceof HttpContent) {
                handleHttpContent(ctx, (HttpContent) msg, queue);
            } else {
                ReferenceCountUtil.release(msg);
            }
        } catch (Exception e) {
            ReferenceCountUtil.release(msg);
            exceptionCaught(ctx, e);
        }
    }

    /* ================= handlers ================= */
    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req, Deque<StreamRequestContext> queue) {
        // 创建上下文并入队
        StreamRequestContext context = new StreamRequestContext(MAX_PENDING_CHUNKS);
        queue.addLast(context);

        EventLoop eventLoop = ctx.channel().eventLoop();
        String serviceKey = UPSTREAM_HOST + ":" + UPSTREAM_PORT;
        log.info("Processing request: {} {} on event loop: {}", req.method(), req.uri(), eventLoop);
        PoolKey poolKey = new PoolKey(serviceKey, UPSTREAM_HOST, UPSTREAM_PORT, "HTTP");

        // 异步获取连接
        acquireAndWriteDownstream(ctx, req, context, eventLoop, poolKey);
    }

    private void acquireAndWriteDownstream(ChannelHandlerContext upstreamCtx, HttpRequest upstreamReq,
                                           StreamRequestContext context, EventLoop eventLoop, PoolKey poolKey) {
        PerEventLoopChannelPoolManager.getInstance()
                .acquire(eventLoop, poolKey)
                .addListener((Future<Channel> f) -> {
                    if (!f.isSuccess()) {
                        // acquire 失败
                        context.close();
                        writeUpstreamError(upstreamCtx, HttpResponseStatus.BAD_GATEWAY, "Backend unavailable");
                        return;
                    }

                    Channel downstream = f.getNow();
                    // 一切 downstream 操作，必须切回 downstream EventLoop
                    downstream.eventLoop().execute(() -> {
                        // 1. channel 必须 active
                        if (!downstream.isActive()) {
                            destroyDownstream(poolKey, downstream);
                            context.close();
                            writeUpstreamError(upstreamCtx, HttpResponseStatus.BAD_GATEWAY, "Downstream inactive");
                            return;
                        }

                        HttpRequest downstreamReq = HttpCopier.copyRequest(upstreamReq, upstreamCtx.channel());
                        // 2. 真正写请求
                        downstream.writeAndFlush(downstreamReq)
                                .addListener(wf -> {
                                    if (!wf.isSuccess()) {
                                        // write 失败，永不复用
                                        context.close();
                                        destroyDownstream(poolKey, downstream);
                                        writeUpstreamError(upstreamCtx, HttpResponseStatus.BAD_GATEWAY, "Failed to forward request");
                                    } else {
                                        // 只有这里才算“下游 ready”
                                        context.markRequestSent();
                                    }
                                });

                        // 3. 构建 relay
                        StreamRelay relay = new StreamRelay(upstreamCtx.channel(), downstream, eventLoop, poolKey);
                        context.bindRelay(relay);
                    });
                });
    }

    private void destroyDownstream(PoolKey key, Channel ch) {
        try {
            PerEventLoopChannelPoolManager.getInstance().destroy(ch.eventLoop(), key, ch);
        } catch (Exception ignore) {
        }
    }

    private void writeUpstreamError(ChannelHandlerContext ctx, HttpResponseStatus status, String msg) {
        if (!ctx.channel().isActive()) return;

        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8));
        resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());

        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }

    private void handleHttpContent(ChannelHandlerContext ctx, HttpContent content, Deque<StreamRequestContext> queue) {
        StreamRequestContext current = queue.peekFirst();
        if (current != null) {
            // HttpContent 到达
            boolean accepted = current.onContent(content);
            if (!accepted) {
                ReferenceCountUtil.release(content);
                writeUpstreamError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "Too many pending chunks");
            } else if (content instanceof LastHttpContent) {
                queue.removeFirst();
            }
        } else {
            // Content 必须释放，防止泄漏
            ReferenceCountUtil.release(content);
            log.warn("Received content without context, dropped.");
        }
    }

    /* ================= error handling ================= */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel inactive: {}", ctx.channel());
        cleanupQueue(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in upstream handler for channel: {}", ctx.channel(), cause);
        cleanupQueue(ctx);
        ctx.close();
    }

    private void cleanupQueue(ChannelHandlerContext ctx) {
        Deque<StreamRequestContext> queue = ctx.channel().attr(REQ_QUEUE).get();
        if (queue != null) {
            queue.forEach(StreamRequestContext::close);
            queue.clear();
        }
    }
}