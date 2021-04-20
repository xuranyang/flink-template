package com.watermark;

import com.model.UserWatermark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /**
         * 老版本需要手动指定
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


//        DataStream<String> inputDataStream = env.readTextFile("src/main/java/com/data/watermark_data.txt");
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 1234);

        DataStream<UserWatermark> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            String userId = fields[0];
            Integer money = Integer.parseInt(fields[1]);
            Long timestamp = Long.valueOf(fields[2]);
            return new UserWatermark(userId, money, timestamp);
        });

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
         */
        // 滚动窗口写法1
//        watermarkDataStream.keyBy(UserWatermark::getUserId).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum("money").print();
        // 滚动窗口写法2
//        watermarkDataStream.keyBy(UserWatermark::getUserId).timeWindow(Time.seconds(10)).sum("money").print();

        /**
         * 示例2
         */
        watermarkDataStream.keyBy(UserWatermark::getUserId).window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
                }).print();

        env.execute();
    }


}
