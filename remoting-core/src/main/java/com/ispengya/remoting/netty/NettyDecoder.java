package com.ispengya.remoting.netty;

import com.ispengya.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(NettyDecoder.class);
    private static final int FRAME_MAX_LENGTH = 16777216; // 16MB

    public NettyDecoder() {
        // lengthFieldOffset = 0
        // lengthFieldLength = 4
        // lengthAdjustment = 0
        // initialBytesToStrip = 4 (remove the length field)
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            return decodeRemotingCommand(frame);
        } catch (Exception e) {
            log.error("Decode exception", e);
            ctx.close();
        } finally {
            if (frame != null) {
                frame.release();
            }
        }
        return null;
    }

    private RemotingCommand decodeRemotingCommand(ByteBuf byteBuf) {
        int headerLength = byteBuf.readInt();
        byte[] headerData = new byte[headerLength];
        byteBuf.readBytes(headerData);

        RemotingCommand cmd = RemotingCommand.headerDecode(headerData);

        int bodyLength = byteBuf.readableBytes();
        if (bodyLength > 0) {
            byte[] body = new byte[bodyLength];
            byteBuf.readBytes(body);
            cmd.setBody(body);
        }
        return cmd;
    }
}
