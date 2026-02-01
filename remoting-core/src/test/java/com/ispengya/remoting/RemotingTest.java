package com.ispengya.remoting;

import com.ispengya.remoting.netty.NettyRemotingClient;
import com.ispengya.remoting.netty.NettyRemotingServer;
import com.ispengya.remoting.netty.NettyRequestProcessor;
import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

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

            RemotingCommand request = RemotingCommand.createRequestCommand(100, null);
            request.setBody("RocketMQ".getBytes());

            RemotingCommand response = client.invokeSync("127.0.0.1:8888", request, 3000);
            System.out.println("Client receive response: " + response);
            System.out.println("Client receive body: " + new String(response.getBody()));

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
