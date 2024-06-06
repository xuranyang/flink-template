package rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // ChannelHandlerContext 用于传输业务数据
        String message = "Hello, server!";
        ByteBuf buffer = ctx.alloc().buffer(message.length());  // ByteBuf是一个存储字节的容器
        buffer.writeBytes(message.getBytes());
        ctx.writeAndFlush(buffer);  // writeAndFlush()方法向服务端发送消息
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 发送成功后，服务端接收到消息并返回处理结果时，ChannelHandler 的channelRead()方法能接收到服务端返回的响应结果消息
        ByteBuf buffer = (ByteBuf) msg;
        try {
            byte[] data = new byte[buffer.readableBytes()];
            buffer.readBytes(data);
            String response = new String(data);
            System.out.println("Received response from server: " + response);
        } finally {
            // 释放ByteBuf资源
            buffer.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // 异常处理
        cause.printStackTrace();
        ctx.close();
    }
}

