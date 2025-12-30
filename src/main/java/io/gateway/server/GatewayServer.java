package io.gateway.server;

import io.gateway.client.PerEventLoopChannelPoolManager;
import io.gateway.config.NettyConfig;
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
import io.netty.handler.timeout.ReadTimeoutHandler;
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
                                // 服务端读超时：若在一定时间内没有从客户端读取数据，可触发并处理
//                                .addLast(new ReadTimeoutHandler(NettyConfig.READ_TIMEOUT_SECONDS_SERVER))
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
            // 先优雅关闭连接池（释放并关闭池中通道）
            PerEventLoopChannelPoolManager.getInstance().shutdown();
            boss.shutdownGracefully().sync();
            workers.shutdownGracefully().sync();
            log.info("GatewayServer resources released");
        } catch (InterruptedException e) {
            log.warn("Interrupted while shutting down GatewayServer", e);
            Thread.currentThread().interrupt();
        }
    }
}