package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRequestProcessor;
import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {
    void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor);
    int localListenPort();
}
