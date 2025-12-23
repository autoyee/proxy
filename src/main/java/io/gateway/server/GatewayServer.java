package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.plugin.PluginChainExecutor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;

/**
 * @author yee
 */
public class GatewayServer {
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers;

    public GatewayServer(int workers) {
        this.workers = new NioEventLoopGroup(workers);
    }

    public void start(int port, PerEventLoopChannelPoolManager poolManager, PluginChainExecutor plugins) throws Exception {
        ServerBootstrap sb = new ServerBootstrap();
        sb.group(boss, workers)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                  ch.pipeline()
                    .addLast(new HttpServerCodec())
                    .addLast(new HttpObjectAggregator(1024 * 1024))
                    .addLast(new UpstreamHandler(poolManager, plugins));
              }
          });
        sb.bind(port).sync();
    }
}