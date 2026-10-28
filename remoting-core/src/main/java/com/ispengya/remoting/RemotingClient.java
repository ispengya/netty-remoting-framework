package com.ispengya.remoting;

import com.ispengya.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) 
            throws InterruptedException, Exception;

    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, Exception;

    /**
     * 注册服务端推送消息监听器。
     *
     * @param code 推送消息码
     * @param listener 监听器
     */
    void registerPushListener(int code, PushListener listener);

    /**
     * 移除服务端推送消息监听器。
     *
     * @param code 推送消息码
     */
    void unregisterPushListener(int code);
}
