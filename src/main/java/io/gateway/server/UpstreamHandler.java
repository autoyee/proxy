package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
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

import java.net.InetSocketAddress;
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
    private static final String UPSTREAM_HOST = "127.0.0.1";
    private static final int UPSTREAM_PORT = 8081;

    public UpstreamHandler(PluginChainExecutor plugins) {
        this.plugins = plugins;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        ctx.channel().attr(REQ_QUEUE).set(new ArrayDeque<>());
    }

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

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req, Deque<StreamRequestContext> queue) {
        // 1. 创建上下文并入队
        StreamRequestContext context = new StreamRequestContext();
        queue.addLast(context);

        EventLoop el = ctx.channel().eventLoop();
        String serviceKey = UPSTREAM_HOST + ":" + UPSTREAM_PORT;
        log.info("Processing request: {} {} on event loop: {}", req.method(), req.uri(), el);

        // 2. 异步获取连接
        PerEventLoopChannelPoolManager.getInstance()
                .acquire(el, UPSTREAM_HOST, UPSTREAM_PORT, serviceKey)
                .addListener((Future<Channel> f) -> {
                    if (!f.isSuccess()) {
                        log.error("Failed to acquire downstream channel for serviceKey: {}", serviceKey, f.cause());
                        // 失败时清理 Context（释放 pending body）并返回 502
                        context.close();
                        queue.remove(context);
                        safeWriteError(ctx, HttpResponseStatus.BAD_GATEWAY, "Backend unavailable");
                        return;
                    }

                    Channel downstream = f.getNow();
                    // 传入 clientChannel 以增强 Header
                    HttpRequest downstreamReq = HttpCopier.copyRequest(req, ctx.channel());
                    log.info("Successfully acquired downstream channel: {} for serviceKey: {}, URL: {} {} ",
                            downstream, serviceKey, downstreamReq.method(), downstreamReq.uri());

                    downstream.writeAndFlush(downstreamReq)
                            .addListener(writeFuture -> {
                                if (writeFuture.isSuccess()) {
                                    log.info("Forwarding httpRequest to downstream success");
                                } else {
                                    log.error("Failed to forward httpRequest to downstream", writeFuture.cause());
                                    // 写失败处理
                                    context.close();
                                    safeWriteError(ctx, HttpResponseStatus.BAD_GATEWAY, "Failed to send request");
                                }
                            });

                    // 3. 绑定 Relay
                    StreamRelay relay = new StreamRelay(ctx.channel(), downstream, el, serviceKey);
                    context.bindRelay(relay);
                });
    }

    private void handleHttpContent(ChannelHandlerContext ctx, HttpContent content, Deque<StreamRequestContext> queue) {
        StreamRequestContext current = queue.peekFirst();
        if (current != null) {
            current.onContent(content);
            if (content instanceof LastHttpContent) {
                queue.removeFirst();
            }
        } else {
            // Content 必须释放，防止泄漏
            ReferenceCountUtil.release(content);
            log.warn("Received content without context, dropped.");
        }
    }

    // 统一错误响应方法
    private void safeWriteError(ChannelHandlerContext ctx, HttpResponseStatus status, String msg) {
        if (ctx.channel().isActive()) {
            FullHttpResponse resp = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, status,
                    Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8)
            );
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
            ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel inactive: {}", ctx.channel());
        cleanupQueue(ctx);
        super.channelInactive(ctx);
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