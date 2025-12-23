package io.gateway.relay;

import io.netty.handler.codec.http.HttpContent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author yee
 */
@Slf4j
public class StreamRequestContext {

    private volatile StreamRelay relay;
    private final Deque<HttpContent> pending = new ArrayDeque<>();

    public void onContent(HttpContent content) {
        if (relay != null) {
            relay.forwardUpstreamContent(content);
        } else {
            pending.addLast(content.retain());
        }
    }

    public void bindRelay(StreamRelay relay) {
        this.relay = relay;
        while (!pending.isEmpty()) {
            relay.forwardUpstreamContent(pending.pollFirst());
        }
    }

    public void close() {
        if (relay != null) {
            relay.close();
        }
        pending.forEach(ReferenceCountUtil::release);
        pending.clear();
    }
}