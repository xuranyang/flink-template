package com.async;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.fromElements("a", "b", "c", "d", "e");

//        int corePoolSize = 1;
        int corePoolSize = 2;
//        int corePoolSize = 5;

        /**
         * AsyncDataStream有2个方法，
         * unorderedWait:不保证输出元素的顺序和读入元素顺序相同
         * 当异步的请求完成时，其结果立马返回，不考虑结果顺序即乱序模式。当以processing time作为时间属性时，该模式可以获得最小的延时和最小的开销
         *
         * orderedWait:保证输出元素的顺序和读入元素顺序相同
         * 该模式下，消息在异步I/O算子前后的顺序一致，先请求的先返回，即有序模式。
         * 为实现有序模式，算子将请求返回的结果放入缓存，直到该请求之前的结果全部返回或超时。
         * 该模式通常情况下回引入额外的时延以及在checkpoint过程中会带来开销，这是因为，和无序模式相比，消息和请求返回的结果都会在checkpoint的状态中维持更长时间
         */
//        AsyncDataStream.orderedWait(dataStream, new UserDefineAsyncFunction(corePoolSize) {
//        }, 60, TimeUnit.SECONDS, 100).print("orderedWait");

        AsyncDataStream.unorderedWait(dataStream, new UserDefineAsyncFunction(corePoolSize) {
        }, 60, TimeUnit.SECONDS, 100).print("unorderedWait");

        env.execute();
    }
}

class UserDefineAsyncFunction extends RichAsyncFunction<String, String> {
    //    transient ExecutorService executorService;
    //    final int numElements = 5;
    transient ThreadPoolExecutor threadPoolExecutor;
    int corePoolSize;

    public UserDefineAsyncFunction(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        executorService = Executors.newFixedThreadPool(numElements);
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, 10, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
    }

    @Override
    public void close() throws Exception {
        super.close();
//        executorService.shutdownNow();
        threadPoolExecutor.shutdownNow();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                try {
//                    int delay = 2;
                    int delay = new Random().nextInt(5);
                    TimeUnit.SECONDS.sleep(delay);
                    System.out.println("延迟:" + delay + "秒|" + "异步处理数据:" + Thread.currentThread().getId() + "|" + input);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                resultFuture.complete(Collections.singletonList(System.currentTimeMillis() + ":" + input));

            }
        });
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
        //超时后的处理
    }

}
