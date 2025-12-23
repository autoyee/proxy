package io.gateway.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.util.concurrent.Future;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yee
 */
public final class PerEventLoopChannelPoolManager {

    private static final PerEventLoopChannelPoolManager INSTANCE = new PerEventLoopChannelPoolManager();

    private final Map<EventLoop, Map<String, FixedChannelPool>> pools = new ConcurrentHashMap<>();

    private PerEventLoopChannelPoolManager() {
    }

    public static PerEventLoopChannelPoolManager getInstance() {
        return INSTANCE;
    }

    public Future<Channel> acquire(EventLoop eventLoop, String host, int port, String serviceKey) {
        Map<String, FixedChannelPool> perService = pools.computeIfAbsent(eventLoop, el -> new ConcurrentHashMap<>());

        FixedChannelPool pool = perService.computeIfAbsent(serviceKey, key -> {
            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoop)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(host, port)
                    .handler(new DownstreamInitializer());

            return new FixedChannelPool(bootstrap, new ChannelPoolHandler() {

                @Override
                public void channelCreated(Channel ch) {
                    ensureBasePipeline(ch);
                }

                @Override
                public void channelReleased(Channel ch) {
                    cleanupPipeline(ch);
                }

                @Override
                public void channelAcquired(Channel ch) throws Exception {
                    ensureBasePipeline(ch);
                }
            }, 10);
        });

        return pool.acquire();
    }

    private void ensureBasePipeline(Channel ch) {
        ChannelPipeline p = ch.pipeline();

        if (p.get("httpCodec") == null) {
            p.addFirst("httpCodec", new HttpClientCodec());
        }
    }

    public void release(EventLoop eventLoop, String serviceKey, Channel channel) {
        FixedChannelPool pool = pools.get(eventLoop).get(serviceKey);
        pool.release(channel);
    }

    private void cleanupPipeline(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        if (p.get("relay") != null) {
            p.remove("relay");
        }
    }
}
