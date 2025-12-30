package io.gateway.util;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

/**
 * 处理标准代理头
 *
 * @author yee
 */
public final class HttpCopier {

    public static HttpRequest copyFullRequest(FullHttpRequest req) {
        DefaultFullHttpRequest copy =
                new DefaultFullHttpRequest(
                        req.protocolVersion(),
                        req.method(),
                        req.uri(),
                        req.content().retainedDuplicate()
                );
        copy.headers().setAll(req.headers());
        return copy;
    }
}
