package com.ispengya.remoting.netty;

import com.ispengya.remoting.InvokeCallback;
import com.ispengya.remoting.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ResponseFuture {
    private final int opaque;
    private final long timeoutMillis;
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final InvokeCallback invokeCallback;

    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
    }
    
    public ResponseFuture(int opaque, long timeoutMillis) {
        this(opaque, timeoutMillis, null);
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.cause != null) {
                invokeCallback.onException(this.cause);
            } else {
                invokeCallback.onComplete(this.responseCommand);
            }
        }
    }

    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public int getOpaque() {
        return opaque;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }
}
