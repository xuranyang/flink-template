package rpc.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyClient {
    /**
     * 创建客户端实例并向服务端发送连接请求
     */
    public static void main(String[] args) throws Exception {
        // 创建EventLoopGroup，用于处理客户端的I/O操作
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            // 创建Bootstrap实例，客户端启动对象
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class) // 设置服务端Channel类型为NioSocketChannel作为通道实现; Channel表示一个连接，可以理解为每一个请求 就是一个Channel
                    .handler(new ChannelInitializer<SocketChannel>() {  // 设置客户端处理
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            // ChannelPipeline 用于保存处理过程需要用到的ChannelHandler 和 ChannelHandlerContext
                            ch.pipeline().addLast(new NettyClientHandler());    // ChannelHandler 核心处理业务就在这里，用于处理业务请求
                        }
                    });

            // 向服务端发送连接请求
            // ChannelFuture：主要用于接收异步I/O操作返回的执行结果; ChannelFuture提供了丰富的方法，用于检查操作的状态、添加监听器以便在操作完成时接收通知，并对操作的结果进行处理。
            ChannelFuture channelFuture = bootstrap.connect("localhost", 8080).sync();
            channelFuture.channel().closeFuture().sync();
        } finally {
            // 优雅地关闭线程
            group.shutdownGracefully();
        }
    }
}

