package rpc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServer {
    /**
     * 创建服务端实例并绑定端口
     */
    public static void main(String[] args) throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();     // 创建boss线程组，用于接收连接
        EventLoopGroup workerGroup = new NioEventLoopGroup();   // 创建worker线程组，用于处理连接上的I/O操作，含有子线程NioEventGroup个数为CPU核数大小的2倍

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();    // 创建ServerBootstrap实例，服务器启动对象
            // 使用链式编程配置参数
            serverBootstrap.group(bossGroup, workerGroup)   // 将boss线程组和worker线程组暂存到ServerBootstrap
                    .channel(NioServerSocketChannel.class)  // 设置服务端Channel类型为NioServerSocketChannel作为通道实现
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new NettyServerHandler());    // 添加ServerHandler到ChannelPipeline，对workerGroup的SocketChannel（客户端）设置处理器
                        }
                    });

            ChannelFuture channelFuture = serverBootstrap.bind(8080).sync();    // 绑定端口并启动服务器，bind方法是异步的，sync方法是等待异步操作执行完成，返回ChannelFuture异步对象
            channelFuture.channel().closeFuture().sync();   // 等待服务器关闭
        } finally {
            bossGroup.shutdownGracefully();     // 优雅地关闭boss线程组
            workerGroup.shutdownGracefully();   // 优雅地关闭worker线程组
        }
    }
}

