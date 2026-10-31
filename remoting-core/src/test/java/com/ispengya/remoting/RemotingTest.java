package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.netty.NettyRemotingServer;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 端到端通信测试:本地启动 server/client,验证同步、异步和未知请求码三条路径。
 */
public class RemotingTest {

    private static final int REQUEST_CODE = 100;
    private static final long TIMEOUT_MILLIS = 3000L;

    private NettyRemotingServer server;
    private NettyRemotingClient client;
    private ExecutorService processorExecutor;
    private AtomicReference<Channel> serverSideChannel;

    @Before
    public void setUp() {
        processorExecutor = Executors.newCachedThreadPool();
        serverSideChannel = new AtomicReference<Channel>();
        server = new NettyRemotingServer(0);
        server.registerProcessor(REQUEST_CODE, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                serverSideChannel.set(ctx.channel());
                RemotingCommand response = RemotingCommand.createResponseCommand(200, "Success");
                String body = request.getBody() == null
                        ? ""
                        : new String(request.getBody(), StandardCharsets.UTF_8);
                response.setBody(("Hello " + body).getBytes(StandardCharsets.UTF_8));
                return response;
            }
        }, processorExecutor);
        server.start();

        client = new NettyRemotingClient();
        client.start();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
        if (processorExecutor != null) {
            processorExecutor.shutdownNow();
        }
    }

    @Test
    public void invokeSyncReturnsServerResponse() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(REQUEST_CODE, null);
        request.setBody("RocketMQ".getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = client.invokeSync(address(), request, TIMEOUT_MILLIS);

        assertEquals(200, response.getCode());
        assertEquals(request.getOpaque(), response.getOpaque());
        assertEquals("Hello RocketMQ", new String(response.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void invokeAsyncInvokesCallback() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(REQUEST_CODE, null);
        request.setBody("RocketMQAsync".getBytes(StandardCharsets.UTF_8));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<RemotingCommand> responseRef = new AtomicReference<RemotingCommand>();
        final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();

        client.invokeAsync(address(), request, TIMEOUT_MILLIS, new InvokeCallback() {
            @Override
            public void onComplete(RemotingCommand response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("异步回调超时", latch.await(5, TimeUnit.SECONDS));
        assertNull(errorRef.get());
        assertNotNull(responseRef.get());
        assertEquals(200, responseRef.get().getCode());
        assertEquals("Hello RocketMQAsync",
                new String(responseRef.get().getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void unknownRequestCodeReturnsErrorResponse() throws Exception {
        RemotingCommand response = client.invokeSync(address(),
                RemotingCommand.createRequestCommand(9999, null), TIMEOUT_MILLIS);

        assertEquals(-1, response.getCode());
        assertTrue(response.getRemark().contains("not supported"));
    }

    @Test
    public void serverPushReachesRegisteredListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<RemotingCommand> received = new AtomicReference<RemotingCommand>();
        client.registerPushListener(300, new PushListener() {
            @Override
            public void onMessage(RemotingCommand message) {
                received.set(message);
                latch.countDown();
            }
        });

        client.invokeSync(address(), RemotingCommand.createRequestCommand(REQUEST_CODE, null), TIMEOUT_MILLIS);
        Channel channel = serverSideChannel.get();
        assertNotNull(channel);

        RemotingCommand push = RemotingCommand.createPushCommand(300);
        push.setBody("push-body".getBytes(StandardCharsets.UTF_8));
        server.push(channel, push);

        assertTrue("推送消息超时", latch.await(5, TimeUnit.SECONDS));
        assertNotNull(received.get());
        assertEquals(300, received.get().getCode());
        assertEquals("push-body", new String(received.get().getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void pushListenerExceptionDoesNotBreakFollowingPushes() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger(0);
        client.registerPushListener(301, new PushListener() {
            @Override
            public void onMessage(RemotingCommand message) {
                if (count.incrementAndGet() == 1) {
                    throw new IllegalStateException("listener failure");
                }
                latch.countDown();
            }
        });

        client.invokeSync(address(), RemotingCommand.createRequestCommand(REQUEST_CODE, null), TIMEOUT_MILLIS);
        Channel channel = serverSideChannel.get();
        assertNotNull(channel);
        server.push(channel, RemotingCommand.createPushCommand(301));
        server.push(channel, RemotingCommand.createPushCommand(301));

        assertTrue("第二条推送未被处理", latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, count.get());
    }

    @Test
    public void loopbackBoundServerRejectsLanAccess() throws Exception {
        NettyRemotingServer loopbackServer = new NettyRemotingServer("127.0.0.1", 0);
        loopbackServer.registerProcessor(REQUEST_CODE, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                return RemotingCommand.createResponseCommand(200, "OK");
            }
        }, processorExecutor);
        try {
            loopbackServer.start();
            int port = loopbackServer.localListenPort();

            // 回环地址可以正常连通
            try (java.net.Socket socket = new java.net.Socket("127.0.0.1", port)) {
                assertTrue(socket.isConnected());
            }

            // 本机非回环地址不应连通；机器没有可用网卡时跳过该半边断言
            String lanIp = detectLanIpv4();
            if (lanIp != null) {
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(new InetSocketAddress(lanIp, port), 500);
                    org.junit.Assert.fail("回环绑定的端口不应从局域网地址连通: " + lanIp);
                } catch (java.io.IOException expected) {
                    // 预期：连接被拒绝或超时（Windows 防火墙对本机局域网地址常为丢弃）
                }
            }
        } finally {
            loopbackServer.shutdown();
        }
    }

    private static String detectLanIpv4() {
        try {
            java.util.Enumeration<java.net.NetworkInterface> interfaces =
                    java.net.NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                java.net.NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback()
                        || networkInterface.isVirtual()) {
                    continue;
                }
                java.util.Enumeration<java.net.InetAddress> addresses =
                        networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    java.net.InetAddress address = addresses.nextElement();
                    if (address instanceof java.net.Inet4Address && !address.isLoopbackAddress()
                            && !address.isLinkLocalAddress() && address.isSiteLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (java.net.SocketException e) {
            return null;
        }
        return null;
    }

    private String address() {
        return "127.0.0.1:" + server.localListenPort();
    }
}
