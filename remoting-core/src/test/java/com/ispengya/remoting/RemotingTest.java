package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.netty.NettyRemotingServer;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class RemotingTest {
    public static void main(String[] args) {
        NettyRemotingServer server = null;
        NettyRemotingClient client = null;
        try {
            // 1. Server
            server = new NettyRemotingServer(8888);
            server.registerProcessor(100, new NettyRequestProcessor() {
                @Override
                public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                    System.out.println("Server receive request: " + request);
                    String requestBody = new String(request.getBody());
                    System.out.println("Server receive body: " + requestBody);

                    RemotingCommand response = RemotingCommand.createResponseCommand(200, "Success");
                    response.setBody(("Hello " + requestBody).getBytes());
                    return response;
                }
            }, Executors.newCachedThreadPool());

            server.start();

            // 2. Client
            client = new NettyRemotingClient();
            client.start();

            // Sync
            RemotingCommand request = RemotingCommand.createRequestCommand(100, null);
            request.setBody("RocketMQ".getBytes());

            RemotingCommand response = client.invokeSync("127.0.0.1:8888", request, 3000);
            System.out.println("Client receive response: " + response);
            System.out.println("Client receive body: " + new String(response.getBody()));

            // Async
            RemotingCommand asyncRequest = RemotingCommand.createRequestCommand(100, null);
            asyncRequest.setBody("RocketMQAsync".getBytes());
            final CountDownLatch asyncLatch = new CountDownLatch(1);
            client.invokeAsync("127.0.0.1:8888", asyncRequest, 3000, new InvokeCallback() {
                @Override
                public void onComplete(RemotingCommand response) {
                    System.out.println("Client receive async response: " + response);
                    System.out.println("Client receive async body: " + new String(response.getBody()));
                    asyncLatch.countDown();
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    asyncLatch.countDown();
                }
            });
            asyncLatch.await();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.shutdown();
            }
            if (server != null) {
                server.shutdown();
            }
        }
    }
}
