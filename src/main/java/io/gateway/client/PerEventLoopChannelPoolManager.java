package io.gateway.client;

import io.gateway.config.GatewayConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Production-grade per EventLoop channel pool manager.
 * <p>
 * 设计原则：
 * 1. Pool 只负责 borrow / return 连接
 * 2. Pool 不保存 Channel 引用
 * 3. Pool 不感知请求是否成功
 * 4. 是否 release 由上层 Relay 决定
 *
 * @author yee
 */
@Slf4j
public final class PerEventLoopChannelPoolManager implements DownstreamChannelPool {

    private static final PerEventLoopChannelPoolManager INSTANCE = new PerEventLoopChannelPoolManager();
    private final Map<EventLoop, Map<PoolKey, FixedChannelPool>> pools = new ConcurrentHashMap<>();

    public static PerEventLoopChannelPoolManager getInstance() {
        return INSTANCE;
    }

    public Future<Channel> acquire(EventLoop el, PoolKey key) {
        return pools.computeIfAbsent(el, e -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, k -> createPool(el, k))
                .acquire();
    }

    public void release(EventLoop el, PoolKey key, Channel ch) {
        if (!ch.isActive()) {
            ch.close();
            return;
        }
        pools.get(el).get(key).release(ch);
    }

    public void destroy(EventLoop el, PoolKey key, Channel ch) {
        ch.close();
    }

    @Override
    public void shutdown() {

    }

    private FixedChannelPool createPool(EventLoop el, PoolKey key) {
        Bootstrap bs = new Bootstrap()
                .group(el)
                .channel(NioSocketChannel.class)
                .remoteAddress(key.host(), key.port());

        return new FixedChannelPool(
                bs,
                new ChannelPoolHandler() {
                    @Override
                    public void channelReleased(Channel ch) throws Exception {

                    }

                    @Override
                    public void channelAcquired(Channel ch) throws Exception {

                    }

                    @Override
                    public void channelCreated(Channel ch) {
                        // initializer
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpClientCodec());
                        p.addLast(new ReadTimeoutHandler(GatewayConfig.DOWNSTREAM_READ_TIMEOUT_SECONDS));
                    }
                },
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                GatewayConfig.ACQUIRE_TIMEOUT_MS,
                GatewayConfig.MAX_CONN,
                Integer.MAX_VALUE
        );
    }
}
