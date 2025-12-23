package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.plugin.PluginChainExecutor;
import io.gateway.relay.StreamRelay;
import io.gateway.relay.StreamRequestContext;
import io.gateway.util.HttpCopier;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author yee
 */
@Slf4j
public class UpstreamHandler extends ChannelInboundHandlerAdapter {
    private final PluginChainExecutor plugins;
    private static final AttributeKey<Deque<StreamRequestContext>> REQ_QUEUE = AttributeKey.valueOf("req_queue");

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
            // ========== 1. 请求头 ==========
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
                log.info("Forwarding to downstream: {} {}", req.method(), req.uri());
                EventLoop el = ctx.channel().eventLoop();

                String host = "127.0.0.1";
                int port = 8081;
                String serviceKey = el + "|" + host + ":" + port;

                log.info("Processing request: {} {} on event loop: {}", req.method(), req.uri(), el);

                PerEventLoopChannelPoolManager poolManager = PerEventLoopChannelPoolManager.getInstance();
                poolManager.acquire(el, host, port, serviceKey)
                        .addListener((Future<Channel> f) -> {
                            if (!f.isSuccess()) {
                                log.error("Failed to acquire downstream channel for serviceKey: {}", serviceKey, f.cause());
                                ctx.close();
                                return;
                            }

                            Channel downstream = f.getNow();
                            log.info("Successfully acquired downstream channel: {} for serviceKey: {}", downstream, serviceKey);

                            StreamRelay relay = new StreamRelay(ctx.channel(), downstream, el, serviceKey);
                            queue.addLast(new StreamRequestContext(relay));

                            HttpRequest downstreamReq = HttpCopier.copyRequest(req);
                            log.info("Forwarded request to downstream: {}, URL: {} {}", downstream, downstreamReq.method(), downstreamReq.uri());
                            downstream.writeAndFlush(downstreamReq);
                        });

                return;
            }

            // ========== 2. 请求体 ==========
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                StreamRequestContext current = queue.peekFirst();
                if (current != null) {
                    StreamRelay relay = current.getRelay();
                    if (relay != null) {
                        log.info("Forwarding content to relay, readable bytes: {}", content.content().readableBytes());
                        relay.forwardUpstreamContent(content);
                    }

                    if (content instanceof LastHttpContent) {
                        queue.removeFirst();
                    }
                } else {
                    ReferenceCountUtil.release(content);
                }
            }
        } catch (Exception e) {
            ReferenceCountUtil.release(msg);
            throw e;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("Channel inactive: {}", ctx.channel());
        Deque<StreamRequestContext> queue = ctx.channel().attr(REQ_QUEUE).get();
        if (queue != null) {
            queue.forEach(rc -> rc.getRelay().close());
            queue.clear();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("Exception in upstream handler for channel: {}", ctx.channel(), cause);
        Deque<StreamRequestContext> queue = ctx.channel().attr(REQ_QUEUE).get();
        if (queue != null) {
            queue.forEach(rc -> rc.getRelay().close());
            queue.clear();
        }
    }
}