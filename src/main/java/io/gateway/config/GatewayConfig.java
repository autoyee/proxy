package io.gateway.config;

/**
 * Centralized Netty configuration constants.
 * 后续可改为从配置文件/环境变量读取。
 *
 * @author yee
 */
public final class GatewayConfig {
    public static final String BACKEND_HOST = "127.0.0.1";
    public static final int BACKEND_PORT = 8081;

    public static final int MAX_REQUEST_SIZE = 32 * 1024 * 1024;
    public static final int MAX_CONN = 512;
    public static final int ACQUIRE_TIMEOUT_MS = 3000;
    public static final int DOWNSTREAM_READ_TIMEOUT_SECONDS = 30;
}
