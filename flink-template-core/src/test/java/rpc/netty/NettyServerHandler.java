package rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;
        try {
            byte[] data = new byte[buffer.readableBytes()];
            buffer.readBytes(data);
            String message = new String(data);
            System.out.println("Received message from client: " + message);

            // 在这里编写服务器端的业务逻辑处理代码

            // 回复客户端
            String response = "Hello, client!";
            ByteBuf responseBuffer = ctx.alloc().buffer(response.length());
            responseBuffer.writeBytes(response.getBytes());
            ctx.writeAndFlush(responseBuffer);
        } finally {
            buffer.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}

