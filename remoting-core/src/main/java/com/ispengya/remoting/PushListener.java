package com.ispengya.remoting;

import com.ispengya.remoting.protocol.RemotingCommand;

/**
 * 服务端主动推送消息监听器。
 *
 * <p>监听器在客户端网络事件循环中执行，不应阻塞；耗时逻辑应由实现方
 * 自行切换到业务线程池。</p>
 */
public interface PushListener {

    /**
     * 处理一条推送消息。
     *
     * @param message 推送消息
     */
    void onMessage(RemotingCommand message);
}
