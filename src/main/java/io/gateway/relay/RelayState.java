package io.gateway.relay;

/**
 * @author yee
 */
enum RelayState {
    INIT,
    STREAMING,
    COMPLETED,
    FAILED,
    CLOSED
}