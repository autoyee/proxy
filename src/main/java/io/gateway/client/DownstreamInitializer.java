package io.gateway.client;

import io.gateway.config.NettyConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.timeout.ReadTimeoutHandler;

import static io.gateway.common.Constants.HTTP_CODEC;
import static io.gateway.common.Constants.READ_TIMEOUT;

/**
 * @author yee
 */
public class DownstreamInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addFirst(HTTP_CODEC, new HttpClientCodec());
        // 读超时：如果在一定时间内没有从下游读取到数据，会抛出 ReadTimeoutException
        ch.pipeline().addLast(READ_TIMEOUT, new ReadTimeoutHandler(NettyConfig.READ_TIMEOUT_SECONDS_CLIENT));
    }
}