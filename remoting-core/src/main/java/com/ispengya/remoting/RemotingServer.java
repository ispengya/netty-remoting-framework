package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {
    void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor);

    /**
     * 向指定连接推送一条消息。
     *
     * <p>调用方负责构造 {@link RemotingCommand#createPushCommand(int)}
     * 创建的消息；连接不可用时返回失败的 future。</p>
     *
     * @param channel 目标连接
     * @param message 推送消息
     * @return 写出结果
     */
    ChannelFuture push(Channel channel, RemotingCommand message);

    int localListenPort();
}
