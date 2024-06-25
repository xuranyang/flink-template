package rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 已经注册到它的 EventLoop 并且能够处理 I/O 时被调用
        super.channelRegistered(ctx);
        System.out.println("NettyServerHandler 执行 channelRegistered");
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 从它的 EventLoop 注销并且无法处理任何 I/O 时被调
        super.channelUnregistered(ctx);
        System.out.println("NettyServerHandler 执行 channelUnregistered");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 处于活动状态时被调用；Channel 已经连接/绑定并且已经就绪
        super.channelActive(ctx);
        System.out.println("NettyServerHandler 执行 channelActive");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 离开活动状态并且不再连接它的远程节点时被调用
        super.channelInactive(ctx);
        System.out.println("NettyServerHandler 执行 channelInactive");
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 上的一个读操作完成时被调用，对通道的读取完成的事件或通知。当读取完成可通知发送方或其他的相关方，告诉他们接受方读取完成
        super.channelReadComplete(ctx);
        System.out.println("NettyServerHandler 执行 channelReadComplete");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // 当 ctx.fireUserEventTriggered()方法被调用时被调用
        super.userEventTriggered(ctx, evt);
        System.out.println("NettyServerHandler 执行 userEventTriggered");
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        // 当 Channel 的可写状态发生改变时被调用。可以通过调用 Channel 的 isWritable()方法来检测 Channel 的可写性
        // 与可写性相关的阈值可以通过 Channel.config().setWriteHighWaterMark()和 Channel.config().setWriteLowWaterMark()方法来设置
        super.channelWritabilityChanged(ctx);
        System.out.println("NettyServerHandler 执行 channelWritabilityChanged");
    }

    // ==========================================================================================
    /**
     * channelRead: 当从 Channel 读取数据时被调用
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("NettyServerHandler 执行 channelRead");
        // 处理接收到的数据 msg
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
            ctx.writeAndFlush(responseBuffer);  // 发送响应消息给客户端
        } finally {
            // 释放ByteBuf资源
            buffer.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("NettyServerHandler 执行 exceptionCaught");
        // 异常处理
        cause.printStackTrace();
        ctx.close();
    }
}

