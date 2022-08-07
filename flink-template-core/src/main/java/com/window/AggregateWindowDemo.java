package com.window;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * AggregateFunction 窗口增量聚合
 *
 * createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
 * <p>
 * add()：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进一步聚合的过程。
 * 方法传入两个参数：当前新到的数据value，和当前的累加器accumulator；
 * 返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之后都会调用这个方法。
 * <p>
 * getResult()：从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，然后再基于这些聚合的状态计算出一个结果进行输出。
 * 比如之前我们提到的计算平均值，就可以把sum和count作为状态放入累加器，而在调用这个方法时相除得到最终结果。这个方法只在窗口要输出结果时调用。
 * <p>
 * merge()：合并两个累加器，并将合并后的状态作为一个累加器返回。
 * 这个方法只在需要合并窗口的场景下才会被调用；
 * 最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。
 */
public class AggregateWindowDemo {
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
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<UserScoreTs>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserScoreTs>() {
            @Override
            public long extractTimestamp(UserScoreTs userScoreTs, long recordTimestamp) {
                return userScoreTs.getTimestamp();
            }
        }));

        userScoreTsDataStream.keyBy(UserScoreTs::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<UserScoreTs, ScoreNum, Double>() {
                    @Override
                    public ScoreNum createAccumulator() {
                        return new ScoreNum(0, 0);
                    }

                    @Override
                    public ScoreNum add(UserScoreTs value, ScoreNum accumulator) {
//                        Integer userScore = accumulator.getUserScore() + value.userScore;
//                        return new ScoreNum(userScore, accumulator.num += 1);
                        accumulator.setUserScore(accumulator.getUserScore() + value.userScore);
                        accumulator.setNum(accumulator.getNum() + 1);
                        return accumulator;
                    }

                    @Override
                    public Double getResult(ScoreNum accumulator) {
                        return Double.valueOf(accumulator.userScore / accumulator.getNum());
                    }

                    @Override
                    public ScoreNum merge(ScoreNum a, ScoreNum b) {
                        return null;
                    }
                }, new WindowFunction<Double, UserScoreWindowEnd, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<Double> input, Collector<UserScoreWindowEnd> out) throws Exception {
                        if (input.iterator().hasNext()) {
                            Double avgScore = input.iterator().next();
                            out.collect(
                                    UserScoreWindowEnd.builder()
                                            .userId(key)
                                            .userScore(avgScore)
                                            .windowEnd(timeWindow.getEnd())
                                            .build()
                            );
                        }
                    }
                }).print("Aggregate滚动窗口增量聚合");

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
    public static class ScoreNum {
        private Integer userScore;
        private Integer num;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class UserScoreWindowEnd {
        private String userId;
        private Double userScore;
        private Long windowEnd;
    }
}
