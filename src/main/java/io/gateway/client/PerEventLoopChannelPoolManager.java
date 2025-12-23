package io.gateway.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yee
 */
public class PerEventLoopChannelPoolManager {
    private final ConcurrentMap<String, FixedChannelPool> pools = new ConcurrentHashMap<>();

    public CompletableFuture<Channel> acquire(EventLoop el, String host, int port) {
        String key = el + "|" + host + ":" + port;
        FixedChannelPool pool = pools.computeIfAbsent(key, k -> createPool(el, host, port));
        CompletableFuture<Channel> f = new CompletableFuture<>();
        pool.acquire().addListener((Future<Channel> cf) -> {
            if (cf.isSuccess()) {
                f.complete(cf.getNow());
            } else {
                f.completeExceptionally(cf.cause());
            }
        });
        return f;
    }

    private FixedChannelPool createPool(EventLoop el, String host, int port) {
        Bootstrap b = new Bootstrap();
        b.group(el).channel(NioSocketChannel.class).remoteAddress(host, port)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new io.netty.handler.codec.http.HttpClientCodec());
                    }
                });
        return new FixedChannelPool(b, new ChannelPoolHandler() {
            @Override
            public void channelReleased(Channel ch) throws Exception {
                // 可留空或添加释放资源逻辑
            }

            @Override
            public void channelAcquired(Channel ch) throws Exception {
                // 可留空或添加获取连接后的处理逻辑
            }

            @Override
            public void channelCreated(Channel ch) throws Exception {
                // 初始化 channel 的逻辑可放在此处
                ch.pipeline().addLast(new io.netty.handler.codec.http.HttpClientCodec());
            }
        }, 20);

    }
}
