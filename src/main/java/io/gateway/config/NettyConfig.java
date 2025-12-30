package io.gateway.config;

/**
 * Centralized Netty configuration constants.
 * 后续可改为从配置文件/环境变量读取。
 *
 * @author yee
 */
public final class NettyConfig {
    private NettyConfig() {
    }

    // 客户端 Bootstrap 连接超时（毫秒）
    public static final int CONNECT_TIMEOUT_MILLIS = 3000;

    // Downstream 读超时（秒）
    public static final int READ_TIMEOUT_SECONDS_CLIENT = 60;

    // Upstream（server accepted connection）读超时（秒）
    public static final int READ_TIMEOUT_SECONDS_SERVER = 60;

    // FixedChannelPool acquire 超时（毫秒）
    public static final long ACQUIRE_TIMEOUT_MILLIS = 5000L;

    // FixedChannelPool max connections per serviceKey
    public static final int POOL_MAX_CONNECTIONS = 100;

    // FixedChannelPool 最大等待获取连接的请求数
    public static final int POOL_MAX_PENDING_ACQUIRES = 200;
}