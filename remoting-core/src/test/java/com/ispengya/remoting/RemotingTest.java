package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.netty.NettyRemotingServer;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    @Before
    public void setUp() {
        processorExecutor = Executors.newCachedThreadPool();
        server = new NettyRemotingServer(0);
        server.registerProcessor(REQUEST_CODE, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
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

    private String address() {
        return "127.0.0.1:" + server.localListenPort();
    }
}
