package io.gateway.client;

import io.gateway.config.NettyConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.gateway.common.Constants.*;

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
public class PerEventLoopChannelPoolManager implements DownstreamChannelPool {

    private static final PerEventLoopChannelPoolManager INSTANCE = new PerEventLoopChannelPoolManager();

    /**
     * EventLoop -> (PoolKey -> FixedChannelPool)
     */
    private final Map<EventLoop, Map<PoolKey, FixedChannelPool>> pools = new ConcurrentHashMap<>();

    private PerEventLoopChannelPoolManager() {
    }

    public static PerEventLoopChannelPoolManager getInstance() {
        return INSTANCE;
    }

    @Override
    public Future<Channel> acquire(EventLoop eventLoop, PoolKey key) {
        FixedChannelPool pool = getOrCreatePool(eventLoop, key);
        return pool.acquire();
    }

    private FixedChannelPool getOrCreatePool(EventLoop eventLoop, PoolKey key) {
        Map<PoolKey, FixedChannelPool> perEl = pools.computeIfAbsent(eventLoop, el -> new ConcurrentHashMap<>());

        return perEl.computeIfAbsent(key, k -> createPool(eventLoop, k));
    }

    private FixedChannelPool createPool(EventLoop eventLoop, PoolKey key) {
        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .remoteAddress(key.host(), key.port())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, NettyConfig.CONNECT_TIMEOUT_MILLIS);

        return new FixedChannelPool(
                bootstrap,
                new ChannelPoolHandler() {
                    @Override
                    public void channelCreated(Channel ch) {
                        ch.pipeline().addFirst(HTTP_CODEC, new HttpClientCodec());
                        // 读超时：如果在一定时间内没有从下游读取到数据，会抛出 ReadTimeoutException
                        ch.pipeline().addLast(READ_TIMEOUT, new ReadTimeoutHandler(NettyConfig.READ_TIMEOUT_SECONDS_CLIENT));
                    }

                    @Override
                    public void channelAcquired(Channel ch) {
                        // no-op
                    }

                    @Override
                    public void channelReleased(Channel ch) {
                        // no-op
                    }
                },
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                NettyConfig.ACQUIRE_TIMEOUT_MILLIS,
                NettyConfig.POOL_MAX_CONNECTIONS,
                NettyConfig.POOL_MAX_PENDING_ACQUIRES
        );
    }

    @Override
    public void release(EventLoop eventLoop, PoolKey key, Channel channel) {
        if (eventLoop == null || key == null || channel == null) {
            destroy(channel);
            return;
        }

        if (!channel.isActive()) {
            destroy(channel);
            return;
        }

        FixedChannelPool pool = getPool(eventLoop, key);
        if (pool == null) {
            destroy(channel);
            return;
        }

        try {
            pool.release(channel);
        } catch (Exception e) {
            log.warn("Failed to release channel to pool {}, closing channel", key, e);
            destroy(channel);
        }
    }

    private FixedChannelPool getPool(EventLoop eventLoop, PoolKey key) {
        Map<PoolKey, FixedChannelPool> perEl = pools.get(eventLoop);
        return perEl != null ? perEl.get(key) : null;
    }

    /**
     * 异常场景调用，永不复用
     */
    @Override
    public void destroy(EventLoop eventLoop, PoolKey key, Channel channel) {
        destroy(channel);
    }

    /**
     * 关闭并释放所有池里的 channel。用于优雅关闭。
     */
    public void shutdown() {
        log.info("Shutting down ChannelPoolManager");

        for (Map<PoolKey, FixedChannelPool> perEl : pools.values()) {
            for (FixedChannelPool pool : perEl.values()) {
                try {
                    pool.close();
                } catch (Throwable ignore) {
                }
            }
            perEl.clear();
        }
        pools.clear();
        log.info("ChannelPoolManager shutdown complete");
    }

    private void destroy(Channel channel) {
        if (channel == null) return;
        try {
            channel.close();
        } catch (Throwable ignore) {
        }
    }
}
