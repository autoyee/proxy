package io.gateway.server;

import io.gateway.plugin.PluginChainExecutor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yee
 */
@Slf4j
public class GatewayServer {
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers;

    public GatewayServer(int workers) {
        if (workers <= 0) {
            throw new IllegalArgumentException("Workers count must be positive");
        }
        this.workers = new NioEventLoopGroup(workers);
    }

    public void start(int port, PluginChainExecutor plugins) throws Exception {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }

        ServerBootstrap sb = new ServerBootstrap();
        sb.group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec())
                                .addLast(new LoggingHandler(LogLevel.DEBUG))
//                    .addLast(new HttpObjectAggregator(1024 * 1024))
                                .addLast(new UpstreamHandler(plugins));
                    }
                });
        try {
            sb.bind(port).sync();
            log.info("GatewayServer started on port {}", port);
        } catch (Exception e) {
            log.error("Failed to start GatewayServer on port {}", port, e);
            throw e;
        }
    }


    public void close() {
        try {
            boss.shutdownGracefully().sync();
            workers.shutdownGracefully().sync();
            log.info("GatewayServer resources released");
        } catch (InterruptedException e) {
            log.warn("Interrupted while shutting down GatewayServer", e);
            Thread.currentThread().interrupt();
        }
    }
}