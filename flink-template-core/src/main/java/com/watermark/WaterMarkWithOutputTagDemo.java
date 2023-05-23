package com.watermark;

import com.model.UserWatermark;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class WaterMarkWithOutputTagDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /**
         * 老版本需要手动指定
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


//        DataStream<String> inputDataStream = env.readTextFile("flink-template-core/src/main/java/com/data/watermark_data2.txt");
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 1234);

        DataStream<UserWatermark> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            String userId = fields[0];
            Integer money = Integer.parseInt(fields[1]);
            Long timestamp = Long.valueOf(fields[2]);
            return new UserWatermark(userId, money, timestamp);
        });

        /**
         * 定义侧输出流
         */
        // 写法一
        OutputTag<UserWatermark> userWatermarkOutputTag = new OutputTag<UserWatermark>("late-data"){};
        // 写法二
//        OutputTag<UserWatermark> userWatermarkOutputTag = new OutputTag<UserWatermark>("late-data", TypeInformation.of(UserWatermark.class));

        /**
         * 老版本提取Watermark写法
         */
//        DataStream<UserWatermark> watermarkDataStream = dataStream.assignTimestampsAndWatermarks(
//                /**
//                 * 处理乱序数据
//                 */
//                new BoundedOutOfOrdernessTimestampExtractor<UserWatermark>(Time.seconds(1)) {
//                    @Override
//                    public long extractTimestamp(UserWatermark userWatermark) {
//                        return userWatermark.getTimestamp();
//                    }
//                });

        /**
         * 新版本提取Watermark写法
         */
        DataStream<UserWatermark> watermarkDataStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserWatermark>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserWatermark>() {
                    @Override
                    public long extractTimestamp(UserWatermark userWatermark, long l) {
                        return userWatermark.getTimestamp();
                    }
                }));

        /**
         * 示例1
         * 允许迟到时间为 20s,
         * watermark 为 1s
         * 滚动窗口大小为 10s
         * 10s + 20s + 1s =31s
         * 若窗口为0-10s
         * 此时到达10s + 1s=11s时 会进行一次输出,但不会立即关闭 之后的每来一条延迟数据都会再输出一次
         * 只有在 窗口开始时间的 31s以后才会真正关闭窗口
         * 31s之后 再出现该窗口的数据 就只会进入侧输出流
         * 3重保障:1.watermark 2.allowedLateness 3.sideOutputLateData
         */
        // 滚动窗口写法1
        SingleOutputStreamOperator<UserWatermark> result = watermarkDataStream
                .keyBy(UserWatermark::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(20))
                .sideOutputLateData(userWatermarkOutputTag)
                .sum("money");

        // 滚动窗口写法2
//        SingleOutputStreamOperator<UserWatermark> result = watermarkDataStream.keyBy(UserWatermark::getUserId).timeWindow(Time.seconds(10))
//                .allowedLateness(Time.seconds(20)).sideOutputLateData(userWatermarkOutputTag).sum("money");

        result.print("normal");
        result.getSideOutput(userWatermarkOutputTag).print("late");

        /**
         * 示例2
         */
        SingleOutputStreamOperator<String> result2 = watermarkDataStream
                .keyBy(UserWatermark::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(20))
                .sideOutputLateData(userWatermarkOutputTag)
                .apply(new WindowFunction<UserWatermark, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<UserWatermark> iterable, Collector<String> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();

                        List<String> list = new ArrayList<>();
                        for (UserWatermark userWatermark : iterable) {
                            list.add(userWatermark.toString());
                            out.collect(userWatermark.toString());
                        }
                        System.out.println(String.format("Start:%s|End:%s|%s",
                                String.valueOf(start), String.valueOf(end), list.toString()));
                    }
                });

//        result2.print("normal");
//        result2.getSideOutput(userWatermarkOutputTag).print("late");

        env.execute();
    }


}
