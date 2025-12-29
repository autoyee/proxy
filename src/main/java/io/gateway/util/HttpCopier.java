package io.gateway.util;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;

import java.net.InetSocketAddress;

/**
 * 处理标准代理头
 *
 * @author yee
 */
public final class HttpCopier {

    public static final String X_FORWARDED_FOR = "X-Forwarded-For";

    private HttpCopier() {
    }

    public static HttpRequest copyRequest(HttpRequest in, Channel clientChannel) {
        DefaultHttpRequest out = new DefaultHttpRequest(in.protocolVersion(), in.method(), in.uri());
        out.headers().set(in.headers());

        removeHopHeaders(out.headers());
        // 添加代理头
        enrichProxyHeaders(out.headers(), clientChannel);

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
        // 强制使用 Chunked，适应流式转发
        h.set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    }

    // 添加 X-Forwarded-For 等头信息
    private static void enrichProxyHeaders(HttpHeaders h, Channel clientChannel) {
        InetSocketAddress remote = (InetSocketAddress) clientChannel.remoteAddress();
        String clientIp = remote.getAddress().getHostAddress();

        // 追加 X-Forwarded-For
        if (h.contains(X_FORWARDED_FOR)) {
            String current = h.get(X_FORWARDED_FOR);
            h.set(X_FORWARDED_FOR, current + ", " + clientIp);
        } else {
            h.set(X_FORWARDED_FOR, clientIp);
        }

        // 设置 X-Real-IP (通常覆盖为直连 IP)
        h.set("X-Real-IP", clientIp);
    }
}