package com.window;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ReduceFunction 窗口增量聚合
 */
public class ReduceWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<UserScoreTs> userScoreTsDataStream = env.fromElements(
                new UserScoreTs("Maybe", 10, 1659867435640L),
                new UserScoreTs("Ame", 40, 1659867435641L),
                new UserScoreTs("Maybe", 20, 1659867435642L),
                new UserScoreTs("Ame", 50, 1659867435643L),
                new UserScoreTs("Maybe", 30, 1659867435644L),
                new UserScoreTs("Chalice", 100, 1659867446644L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<UserScoreTs>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<UserScoreTs>() {
                    @Override
                    public long extractTimestamp(UserScoreTs userScoreTs, long recordTimestamp) {
                        return userScoreTs.getTimestamp();
                    }
                })
        );

        userScoreTsDataStream.keyBy(UserScoreTs::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserScoreTs>() {
                    @Override
                    public UserScoreTs reduce(UserScoreTs value1, UserScoreTs value2) throws Exception {
                        return UserScoreTs.builder()
                                .userId(value1.getUserId())
                                .userScore(value1.getUserScore() + value2.getUserScore())
                                .timestamp(value1.getTimestamp())
                                .build();
                    }
                }, new WindowFunction<UserScoreTs, UserScoreWindowEnd, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<UserScoreTs> input, Collector<UserScoreWindowEnd> out) throws Exception {
//                        System.out.println(key + "||" + timeWindow.getEnd() + "||" + Lists.newArrayList(input.iterator()));
                        if (input.iterator().hasNext()) {
                            UserScoreTs userScoreTs = input.iterator().next();
                            out.collect(UserScoreWindowEnd.builder()
                                    .userId(key)
                                    .userScore(userScoreTs.getUserScore())
                                    .windowEnd(timeWindow.getEnd())
                                    .build()
                            );
                        }
                    }
                }).print("Reduce滚动窗口增量聚合");

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class UserScoreTs {
        private String userId;
        private Integer userScore;
        private Long timestamp;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class UserScoreWindowEnd {
        private String userId;
        private Integer userScore;
        private Long windowEnd;
    }
}
