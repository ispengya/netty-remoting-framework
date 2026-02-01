package com.ispengya.remoting;

import com.ispengya.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) 
            throws InterruptedException, Exception;

    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, Exception;
}
