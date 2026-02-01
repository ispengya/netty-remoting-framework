package com.ispengya.remoting;

import com.ispengya.remoting.protocol.RemotingCommand;

public interface InvokeCallback {
    void onComplete(final RemotingCommand response);
    void onException(final Throwable e);
}
