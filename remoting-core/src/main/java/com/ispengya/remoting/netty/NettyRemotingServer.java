package com.ispengya.remoting.netty;

import com.ispengya.remoting.RemotingServer;
import com.ispengya.remoting.common.Pair;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupBoss;
    private final EventLoopGroup eventLoopGroupSelector;
    private final int port;
    private int listenPort;

    public NettyRemotingServer(int port) {
        this(port, 1, 4);
    }

    public NettyRemotingServer(int port, int bossThreads, int selectorThreads) {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupBoss = new NioEventLoopGroup(bossThreads, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
            }
        });

        this.eventLoopGroupSelector = new NioEventLoopGroup(selectorThreads, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettySelector_%d", this.threadIndex.incrementAndGet()));
            }
        });
        this.port = port;
    }

    @Override
    public void start() {
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 65535)
                .childOption(ChannelOption.SO_RCVBUF, 65535)
                .localAddress(new InetSocketAddress(this.port))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyDecoder(),
                                new NettyEncoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyServerHandler()
                        );
                    }
                });

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.listenPort = addr.getPort();
            log.info("RemotingServer started, listening on {}", this.listenPort);
        } catch (InterruptedException e) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e);
        }
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupBoss.shutdownGracefully();
        this.eventLoopGroupSelector.shutdownGracefully();
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        this.processorTable.put(requestCode, new Pair<>(processor, executorThis));
    }

    @Override
    public ChannelFuture push(Channel channel, RemotingCommand message) {
        if (channel == null) {
            throw new IllegalArgumentException("Channel must not be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message must not be null");
        }
        if (!channel.isActive()) {
            return channel.newFailedFuture(new IllegalStateException("Channel is not active"));
        }
        return channel.writeAndFlush(message);
    }

    @Override
    public int localListenPort() {
        return this.listenPort;
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }
}
