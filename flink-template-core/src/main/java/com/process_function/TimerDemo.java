package com.process_function;

import com.model.UserWatermark;
import com.util.FlinkUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(0L);     // 新版本,如果间隔设置为 0 表示使用 处理时间
//        env.getConfig().setAutoWatermarkInterval(200L);   // 新版本,如果间隔设置不为 0 表示使用 事件时间


        DataStreamSource<Integer> dataStreamSource = env.addSource(new RichSourceFunction<Integer>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                Integer num = 10;
                while (flag) {
                    ctx.collect(num);
                    TimeUnit.MILLISECONDS.sleep(500);
                    num--;
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // forMonotonousTimestamps 等价于 forBoundedOutOfOrderness 的乱序时间为0的情况
        DataStream<Integer> dataStream = dataStreamSource
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() {
                    @Override
                    public long extractTimestamp(Integer element, long previousElementTimestamp) {
                        // previousElementTimestamp 为 上一个元素的时间戳(如果尚未分配时间戳，则为负值)
                        // 使用 Kafka 的时间戳时，无需定义时间戳提取器 extractTimestamp() 方法的 previousElementTimestamp 参数包含 Kafka 消息携带的时间戳
                        long currentTimestamp = System.currentTimeMillis();
                        System.out.println("提取时间戳:" + currentTimestamp + "||" + previousElementTimestamp);
                        return currentTimestamp;
                    }
                }));

        dataStream.keyBy((KeySelector<Integer, String>) integer -> {
            if (integer % 2 == 0) {
                return "EVEN";
            } else {
                return "ODD";
            }
        }).process(new KeyedProcessFunction<String, Integer, String>() {
            @Override
            public void processElement(Integer element, Context context, Collector<String> out) throws Exception {
                /**
                 * context.timestamp() -> extractTimestamp中的currentTimestamp
                 */
                String currentKey = context.getCurrentKey();

                System.out.println("=====================================================================");
                System.out.println(currentKey + "当前时间：" + context.timestamp());
//                context.timerService().registerProcessingTimeTimer(context.timestamp() + 3000L); // 延迟3秒
                context.timerService().registerEventTimeTimer(context.timestamp() + 3000L); // 延迟3秒
//                context.timerService().deleteEventTimeTimer(context.timestamp() + 3000L); // 删除定时器
                System.out.println(currentKey + "注册了一个定时器：" + (context.timestamp() + 3000L));
                System.out.println("=====================================================================");
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                // ctx.timestamp() == timestamp
                // 当前时间戳 - timestamp = Duration.ofSeconds(1)
                System.out.println(ctx.getCurrentKey() + "定时器触发:" + ctx.timestamp() + "||" + timestamp + "||" + System.currentTimeMillis());
            }
        }).print();

        env.execute();
    }
}
