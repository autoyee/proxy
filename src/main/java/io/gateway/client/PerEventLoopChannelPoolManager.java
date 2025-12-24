package io.gateway.client;

import io.gateway.config.NettyConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.gateway.common.Constants.HTTP_CODEC;
import static io.gateway.common.Constants.RELAY;

/**
 * @author yee
 */
@Slf4j
public class PerEventLoopChannelPoolManager {

    private static final PerEventLoopChannelPoolManager INSTANCE = new PerEventLoopChannelPoolManager();

    // EventLoop -> (serviceKey -> PoolHolder)
    private final Map<EventLoop, Map<String, PoolHolder>> POOLS = new ConcurrentHashMap<>();

    private PerEventLoopChannelPoolManager() {
    }

    public static PerEventLoopChannelPoolManager getInstance() {
        return INSTANCE;
    }

    public Future<Channel> acquire(EventLoop eventLoop, String host, int port, String serviceKey) {
        Map<String, PoolHolder> perService = POOLS.computeIfAbsent(eventLoop, el -> new ConcurrentHashMap<>());

        PoolHolder holder = perService.computeIfAbsent(serviceKey, key -> {
            final PoolHolder ph = new PoolHolder();
            ph.pool = createFixedChannelPool(eventLoop, host, port, ph);
            return ph;
        });

        // 防御性检查，确保pool已设置
        if (holder.pool == null) {
            holder.pool = createFixedChannelPool(eventLoop, host, port, holder);
        }

        return holder.pool.acquire();
    }

    /**
     * 创建FixedChannelPool的私有方法，避免代码重复
     */
    private FixedChannelPool createFixedChannelPool(EventLoop eventLoop, String host, int port, PoolHolder holder) {
        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port)
                // 设置连接超时（毫秒）
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, NettyConfig.CONNECT_TIMEOUT_MILLIS)
                .handler(new DownstreamInitializer());

        // 使用健康检查、acquire 超时策略，和 maxConnections
        return new FixedChannelPool(
                bootstrap,
                new ChannelPoolHandler() {
                    @Override
                    public void channelCreated(Channel ch) {
                        ensureBasePipeline(ch);
                        holder.channels.add(ch);
                    }

                    @Override
                    public void channelReleased(Channel ch) {
                        cleanupPipeline(ch);
                    }

                    @Override
                    public void channelAcquired(Channel ch) {
                        ensureBasePipeline(ch);
                    }
                },
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                NettyConfig.ACQUIRE_TIMEOUT_MILLIS,
                NettyConfig.POOL_MAX_CONNECTIONS,
                NettyConfig.POOL_MAX_PENDING_ACQUIRES
        );
    }

    private void ensureBasePipeline(Channel ch) {
        ChannelPipeline p = ch.pipeline();

        if (p.get(HTTP_CODEC) == null) {
            p.addFirst(HTTP_CODEC, new HttpClientCodec());
        }
    }

    public void release(EventLoop eventLoop, String serviceKey, Channel channel) {
        if (eventLoop == null || serviceKey == null || channel == null) {
            if (channel != null && channel.isActive()) {
                channel.close();
            }
            return;
        }
        Map<String, PoolHolder> perService = POOLS.get(eventLoop);
        if (perService == null) {
            // no pool for this event loop: close channel to be safe
            if (channel.isActive()) channel.close();
            return;
        }
        PoolHolder holder = perService.get(serviceKey);
        if (holder == null || holder.pool == null) {
            if (channel.isActive()) channel.close();
            return;
        }
        try {
            holder.pool.release(channel);
        } catch (Exception e) {
            log.warn("Failed to release channel to pool for serviceKey {}, closing channel: {}", serviceKey, e.getMessage());
            if (channel.isActive()) channel.close();
        }
    }

    private void cleanupPipeline(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        if (p.get(RELAY) != null) {
            p.remove(RELAY);
        }
    }

    /**
     * 关闭并释放所有池里的 channel。用于优雅关闭。
     */
    public void shutdown() {
        log.info("Shutting down PerEventLoopChannelPoolManager, closing pooled channels.");
        for (Map.Entry<EventLoop, Map<String, PoolHolder>> entry : POOLS.entrySet()) {
            Map<String, PoolHolder> perService = entry.getValue();
            if (perService == null) continue;
            for (Map.Entry<String, PoolHolder> holderEntry : perService.entrySet()) {
                PoolHolder holder = holderEntry.getValue();
                if (holder == null) continue;
                for (Channel ch : holder.channels) {
                    try {
                        if (ch != null && ch.isActive()) {
                            ch.close().sync();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception ex) {
                        log.warn("Error closing pooled channel: {}", ex.getMessage());
                    }
                }
                holder.channels.clear();
            }
            perService.clear();
        }
        POOLS.clear();
        log.info("PerEventLoopChannelPoolManager shutdown complete.");
    }

    private static final class PoolHolder {
        volatile FixedChannelPool pool;
        final Set<Channel> channels = ConcurrentHashMap.newKeySet();

        PoolHolder() {
        }

        PoolHolder(FixedChannelPool pool) {
            this.pool = pool;
        }
    }
}
