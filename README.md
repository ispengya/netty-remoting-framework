# Netty Remoting Framework

基于 Netty 实现的远程通信框架，参考了 RocketMQ 的 Remoting 模块设计。

## Maven

运行时要求 Java 8+。

Maven Central:[io.github.ispengya:remoting-core:0.1.0](https://repo1.maven.org/maven2/io/github/ispengya/remoting-core/0.1.0/)

```xml
<dependency>
  <groupId>io.github.ispengya</groupId>
  <artifactId>remoting-core</artifactId>
  <version>0.1.0</version>
</dependency>
```

## 项目结构

- `remoting-core`: 核心模块，包含服务端、客户端、协议编解码等实现。

## 功能特性

- **自定义协议**: Header (JSON) + Body (Bytes)
- **Netty 通信**: 基于 Netty 4.1.x，支持 NIO
- **同步调用**: 客户端支持 `invokeSync` 同步等待响应
- **异步调用**: 客户端支持 `invokeAsync` 回调
- **可扩展 Processor**: 服务端支持注册不同 Request Code 的业务处理器

## 快速开始

### 启动服务端

```java
NettyRemotingServer server = new NettyRemotingServer(8888);
server.registerProcessor(100, new NettyRequestProcessor() {
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        // 处理请求
        RemotingCommand response = RemotingCommand.createResponseCommand(200, "Success");
        response.setBody("Hello".getBytes());
        return response;
    }
}, Executors.newCachedThreadPool());
server.start();
```

### 启动客户端

```java
NettyRemotingClient client = new NettyRemotingClient();
client.start();

RemotingCommand request = RemotingCommand.createRequestCommand(100, null);
request.setBody("Test".getBytes());
RemotingCommand response = client.invokeSync("127.0.0.1:8888", request, 3000);
```

## 许可证

[MIT](LICENSE)
