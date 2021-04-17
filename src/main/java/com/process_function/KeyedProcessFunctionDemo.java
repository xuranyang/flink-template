package com.process_function;

import com.model.UserPos;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 最底层的API
 * ProcessFunction:
 * 可以访问时间戳、watermark
 * 可以注册定时事件
 * 可以处理迟到事件
 */
public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.readTextFile("src/main/java/com/data/data2.txt");

        dataStream.map(new MapFunction<String, UserPos>() {
            @Override
            public UserPos map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserPos(split[0], split[1], split[2]);
            }
        }).keyBy(new KeySelector<UserPos, String>() {
            @Override
            public String getKey(UserPos userPos) throws Exception {
                return userPos.getClub();
            }
        }).process(new UserDefineProcessFunction()).print();

        env.execute();
    }


    // KeyType InputType OuputType
    public static class UserDefineProcessFunction extends KeyedProcessFunction<String, UserPos, Long> {

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-TimerState", Long.class));
        }

        @Override
        public void processElement(UserPos value, Context ctx, Collector<Long> out) throws Exception {


            // 获取当前的事件时间戳(13位,精确到毫秒),相对来说意义不大
            // 当前方法内,时间戳大小不会改变
            ctx.timestamp();

            // 获取当前的Key
            ctx.getCurrentKey();

            // 侧输出流输出,用于分流
//            ctx.output();

            /**
             * ProcessTime
             */
            // 如果是处理时间,可以获取当前的处理时间
            // 会变,不同时刻获取的时间戳大小不同
            ctx.timerService().currentProcessingTime();
            // 所有定时器传的参数 都是13位的timestamp
            // 当前处理时间5秒以后触发定时器
            long ts = ctx.timerService().currentProcessingTime() + 5 * 1000L;
            ctx.timerService().registerProcessingTimeTimer(ts);
            // 删除 当前处理时间5秒以后触发的定时器
            ctx.timerService().deleteProcessingTimeTimer(ts);

            /**
             * EventTime
             */
            //如果是事件时间,可以获取当前的watermark
            ctx.timerService().currentWatermark();
            // 当前时间10秒以后触发定时器
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
            // 删除 当前时间10秒以后触发的定时器
            ctx.timerService().deleteEventTimeTimer(ctx.timestamp() + 10 * 1000L);


            /**
             * 使用state存储 定时器时间
             */
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 10 * 1000L);
            ctx.timerService().registerProcessingTimeTimer(tsTimerState.value());
            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());

        }


        /**
         * 触发定时器时所做的操作
         *
         * @param timestamp 触发定时器时的时间戳
         * @param ctx       上下文
         * @param out       输出
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
            System.out.println("定时器触发");
//            ctx.getCurrentKey();
//            ctx.timeDomain();
//            ctx.output();

        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}

