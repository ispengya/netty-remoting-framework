package com.ispengya.remoting.netty;

import com.ispengya.remoting.InvokeCallback;
import com.ispengya.remoting.common.Pair;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public abstract class NettyRemotingAbstract {
    private static final Logger log = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);
    protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE:
                    processResponseCommand(ctx, msg);
                    break;
                case PUSH:
                    processPushCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 处理服务端主动推送消息。
     *
     * <p>默认实现只记录日志，客户端通过覆写该方法分发到注册的监听器。</p>
     *
     * @param ctx 通道上下文
     * @param cmd 推送消息
     */
    protected void processPushCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        log.warn("receive push message but no listener registered, code={}", cmd.getCode());
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.processorTable.get(null) : matched;

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        final NettyRequestProcessor processor = pair.getObject1();
                        RemotingCommand response = processor.processRequest(ctx, cmd);

                        if (!cmd.isResponseType()) {
                            if (response != null) {
                                response.setOpaque(cmd.getOpaque());
                                response.markResponseType();
                                ctx.writeAndFlush(response);
                            }
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        if (!cmd.isResponseType()) {
                            RemotingCommand response = RemotingCommand.createResponseCommand(-1, e.getMessage(), cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            if (pair.getObject2() != null) {
                try {
                    pair.getObject2().submit(run);
                } catch (RejectedExecutionException e) {
                    if (!cmd.isResponseType()) {
                        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "system busy", cmd.getOpaque());
                        ctx.writeAndFlush(response);
                    }
                }
            } else {
                run.run();
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            RemotingCommand response = RemotingCommand.createResponseCommand(-1, error, cmd.getOpaque());
            ctx.writeAndFlush(response);
        }
    }

    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        int opaque = cmd.getOpaque();
        ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.putResponse(cmd);
            if (responseFuture.getInvokeCallback() != null) {
                try {
                    responseFuture.executeInvokeCallback();
                } catch (Throwable e) {
                    log.error("execute callback exception", e);
                }
            }
            responseTable.remove(opaque);
        } else {
            log.warn("receive response, but not matched any request, " + ctx.channel().remoteAddress());
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, Exception {
        final int opaque = request.getOpaque();

        try {
            final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis);
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress addr = channel.remoteAddress();

            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }
                    responseTable.remove(opaque);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new Exception("send request OK, but wait response timeout", responseFuture.getCause());
                } else {
                    throw new Exception("send request failed", responseFuture.getCause());
                }
            }
            return responseCommand;
        } finally {
            this.responseTable.remove(opaque);
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, Exception {
        final int opaque = request.getOpaque();
        final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, invokeCallback);
        this.responseTable.put(opaque, responseFuture);
        final SocketAddress addr = channel.remoteAddress();

        channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (f.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                    return;
                } else {
                    responseFuture.setSendRequestOK(false);
                }
                responseTable.remove(opaque);
                responseFuture.setCause(f.cause());
                responseFuture.putResponse(null);
                try {
                    responseFuture.executeInvokeCallback();
                } catch (Throwable e) {
                    log.warn("execute callback in writeAndFlush addListener, and callback throw", e);
                }
            }
        });
    }

    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                rf.setCause(new Exception("timeout"));
                rf.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }
}
