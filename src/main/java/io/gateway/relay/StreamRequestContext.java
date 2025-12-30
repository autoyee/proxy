package io.gateway.relay;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author yee
 */
@Slf4j
public final class StreamRequestContext {

    private volatile StreamRelay relay;
    private volatile boolean requestSent = false;
    private final Deque<HttpContent> pending;
    private final int maxPending;

    public StreamRequestContext(int maxPending) {
        this.maxPending = maxPending;
        this.pending = new ArrayDeque<>(maxPending);
    }
    public void bindRelay(StreamRelay relay) {
        this.relay = relay;
        while (!pending.isEmpty()) {
            relay.forwardRequest(pending.pollFirst());
        }
    }

    public void markRequestSent() {
        this.requestSent = true;
    }

    public boolean onContent(HttpContent content) {
        if (relay != null && requestSent) {
            relay.forwardRequest(content);
        } else {
            if (pending.size() >= maxPending) {
                log.warn("Pending queue is full, rejecting content. Current size: {}, Max: {}", pending.size(), maxPending);
                return false;
            }
            pending.addLast(content.retain());
        }
        return true;
    }

    public void close() {
        if (relay != null) {
            relay.close();
        }
        pending.forEach(ReferenceCountUtil::release);
        pending.clear();
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String msg) {
        if (!ctx.channel().isActive()) return;

        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8));
        resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }
}