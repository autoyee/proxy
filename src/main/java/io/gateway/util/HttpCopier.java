package io.gateway.util;

import io.netty.handler.codec.http.*;

/**
 * @author yee
 */
public final class HttpCopier {
    public static HttpRequest copyRequest(HttpRequest in) {
        DefaultHttpRequest out = new DefaultHttpRequest(in.protocolVersion(), in.method(), in.uri());
        out.headers().set(in.headers());
        removeHopHeaders(out.headers());
        return out;
    }

    public static HttpResponse copyResponse(HttpResponse in) {
        DefaultHttpResponse out = new DefaultHttpResponse(in.protocolVersion(), in.status());
        out.headers().set(in.headers());
        removeHopHeaders(out.headers());
        return out;
    }

    private static void removeHopHeaders(HttpHeaders h) {
        h.remove(HttpHeaderNames.CONNECTION);
        h.remove(HttpHeaderNames.TRANSFER_ENCODING);
        h.remove(HttpHeaderNames.CONTENT_LENGTH);
    }
}