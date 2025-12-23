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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress remoteAddr = (InetSocketAddress) ctx.channel().remoteAddress();
        String clientIP = remoteAddr.getAddress().getHostAddress();
        int clientPort = remoteAddr.getPort();
        log.info("Client connected {}:{}", clientIP, clientPort);

        super.channelActive(ctx);
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
                // 1.立刻创建 context 并入队
                StreamRequestContext context = new StreamRequestContext();
                queue.addLast(context);

                HttpRequest req = (HttpRequest) msg;
                EventLoop el = ctx.channel().eventLoop();
                String host = "127.0.0.1";
                int port = 8081;
                String serviceKey = el.hashCode() + "|" + host + ":" + port;
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
                            HttpRequest downstreamReq = HttpCopier.copyRequest(req);
                            log.info("Successfully acquired downstream channel: {} for serviceKey: {}, URL: {} {} ",
                                    downstream, serviceKey, downstreamReq.method(), downstreamReq.uri());
                            downstream.writeAndFlush(downstreamReq)
                                    .addListener(future -> {
                                        if (future.isSuccess()) {
                                            log.info("Forwarding HttpRequest to downstream success");
                                        } else {
                                            log.error("Failed to forward HttpRequest to downstream", future.cause());
                                        }
                                    });

                            // 2.绑定 relay，并 flush pending body
                            StreamRelay relay = new StreamRelay(ctx.channel(), downstream, el, serviceKey);
                            context.bindRelay(relay);
                        });

                return;
            }

            // ========== 2. 请求体 ==========
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                StreamRequestContext current = queue.peekFirst();
                if (current != null) {
                    current.onContent(content);

                    // 请求完成，移除队头
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
            queue.forEach(StreamRequestContext::close);
            queue.clear();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("Exception in upstream handler for channel: {}", ctx.channel(), cause);
        Deque<StreamRequestContext> queue = ctx.channel().attr(REQ_QUEUE).get();
        if (queue != null) {
            queue.forEach(StreamRequestContext::close);
            queue.clear();
        }
    }
}