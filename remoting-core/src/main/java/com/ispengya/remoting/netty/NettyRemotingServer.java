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
    /**
     * 绑定地址；null 或空字符串表示绑定所有网卡（通配地址）。
     */
    private final String host;
    private final int port;
    private int listenPort;

    public NettyRemotingServer(int port) {
        this(null, port, 1, 4);
    }

    public NettyRemotingServer(String host, int port) {
        this(host, port, 1, 4);
    }

    public NettyRemotingServer(int port, int bossThreads, int selectorThreads) {
        this(null, port, bossThreads, selectorThreads);
    }

    public NettyRemotingServer(String host, int port, int bossThreads, int selectorThreads) {
        this.host = host;
        this.port = port;
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
                .localAddress(bindAddress())
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
            log.info("RemotingServer started, listening on {}:{}", addr.getAddress().getHostAddress(),
                    this.listenPort);
        } catch (InterruptedException e) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e);
        }
    }

    /**
     * 解析实际绑定地址：未指定 host 时绑定通配地址（所有网卡）。
     */
    private InetSocketAddress bindAddress() {
        if (this.host == null || this.host.trim().isEmpty()) {
            return new InetSocketAddress(this.port);
        }
        return new InetSocketAddress(this.host.trim(), this.port);
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupBoss.shutdownGracefully();
        this.eventLoopGroupSelector.shutdownGracefully();
    }

    /**
     * 等待 shutdown 发起的优雅停机完成。
     *
     * @param timeoutMillis 总等待超时（毫秒）
     * @return 两个 event loop 是否都在超时前退出
     * @throws InterruptedException 等待被中断
     */
    public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        boolean boss = this.eventLoopGroupBoss.awaitTermination(
                timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        long remaining = Math.max(1L, deadline - System.currentTimeMillis());
        boolean selector = this.eventLoopGroupSelector.awaitTermination(
                remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
        return boss && selector;
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
