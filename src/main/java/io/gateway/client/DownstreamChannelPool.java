package io.gateway.client;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

/**
 * @author yee
 */
public interface DownstreamChannelPool {

    Future<Channel> acquire(EventLoop eventLoop, PoolKey key);

    void release(EventLoop eventLoop, PoolKey key, Channel channel);

    void destroy(EventLoop eventLoop, PoolKey key, Channel channel);

    void shutdown();
}