package com.ispengya.remoting.netty;

import com.ispengya.remoting.RemotingClient;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final ConcurrentHashMap<String, Channel> channelTable = new ConcurrentHashMap<>();

    public NettyRemotingClient() {
        this.eventLoopGroupWorker = new NioEventLoopGroup(4, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientWorker_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public void start() {
        this.bootstrap.group(this.eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyDecoder(),
                                new NettyEncoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyClientHandler());
                    }
                });
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupWorker.shutdownGracefully();
        for (Channel channel : this.channelTable.values()) {
            channel.close();
        }
        this.channelTable.clear();
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                return this.invokeSyncImpl(channel, request, timeoutMillis);
            } catch (Exception e) {
                log.error("invokeSync exception", e);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new Exception("connect to <" + addr + "> failed");
        }
    }

    private void closeChannel(String addr, Channel channel) {
        if (channel != null) {
            final String addrRemote = addr == null ? (channel.remoteAddress() != null ? channel.remoteAddress().toString() : "") : addr;
            try {
                if (this.channelTable.get(addrRemote) == channel) {
                    this.channelTable.remove(addrRemote);
                }
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        channel.close().isSuccess());
            } catch (Exception e) {
                log.warn("closeChannel: close the connection to remote address[{}] exception", addrRemote, e);
            }
        }
    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        Channel channel = this.channelTable.get(addr);
        if (channel != null && channel.isActive()) {
            return channel;
        }
        return this.createChannel(addr);
    }

    private Channel createChannel(final String addr) throws InterruptedException {
        // Simplified channel creation, better to use lock or Future to avoid concurrent connection creation
        synchronized (this.channelTable) {
             Channel channel = this.channelTable.get(addr);
             if (channel != null && channel.isActive()) {
                 return channel;
             }
             
             String[] s = addr.split(":");
             ChannelFuture channelFuture = this.bootstrap.connect(s[0], Integer.parseInt(s[1]));
             if (channelFuture.await(3000, TimeUnit.MILLISECONDS)) {
                 if (channelFuture.isSuccess()) {
                     channel = channelFuture.channel();
                     this.channelTable.put(addr, channel);
                     return channel;
                 } else {
                     log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.cause());
                 }
             } else {
                 log.warn("createChannel: connect remote host[" + addr + "] timeout");
             }
        }
        return null;
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }
}
