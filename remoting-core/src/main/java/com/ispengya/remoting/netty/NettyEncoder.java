package com.ispengya.remoting.netty;

import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger log = LoggerFactory.getLogger(NettyEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        try {
            // 1. Header encode
            byte[] headerData = remotingCommand.headerEncode();
            
            // 2. Body
            byte[] body = remotingCommand.getBody();
            int bodyLength = body != null ? body.length : 0;
            
            // 3. Calculate total length
            // Total Length = Header Length Size (4) + Header Data Size + Body Size
            int length = 4 + headerData.length + bodyLength;
            
            // 4. Write data
            out.writeInt(length);
            out.writeInt(headerData.length);
            out.writeBytes(headerData);
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.error("Encode exception", e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            ctx.close();
        }
    }
}
