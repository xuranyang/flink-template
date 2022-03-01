package com.demo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
//        env.setParallelism(2);
        env.enableCheckpointing(30000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 8888);
        DataStream<String> inputDataStream = env.readTextFile("flink-template-core/src/main/java/com/data/agg_data.txt");

        SingleOutputStreamOperator<String> dataStream = inputDataStream.filter(s -> !s.startsWith("-") && !s.isEmpty()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                return Long.valueOf(s.split(",")[2]);
            }
        });

        SingleOutputStreamOperator<UserScoreStat> aggregate = dataStream.map(s -> {
            String[] split = s.split(",");
            return new UserScoreTs(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
        }).keyBy(UserScoreTs::getUserId)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .aggregate(new IncrAggScore(), new WindowItemIncrAggResult());

        aggregate.print();

        env.execute();

    }

    // 增量聚合
    public static class IncrAggScore implements AggregateFunction<UserScoreTs, Tuple2<Long, Long>, ScoreStat> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(UserScoreTs value, Tuple2<Long, Long> accumulator) {
//            System.out.println("[Add]:" + value);
            return Tuple2.of(accumulator.f0 + value.Score, accumulator.f1 + 1);
        }

        @Override
        public ScoreStat getResult(Tuple2<Long, Long> accumulator) {
            return new ScoreStat(accumulator.f0, accumulator.f1, (double) (accumulator.f0 / accumulator.f1));
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    public static class WindowItemIncrAggResult implements WindowFunction<ScoreStat, UserScoreStat, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<ScoreStat> input, Collector<UserScoreStat> out) throws Exception {
            // 按userId 做keyBy
            String userId = key;
            Long windowEnd = window.getEnd();
            ScoreStat scoreStat = input.iterator().next();
            out.collect(new UserScoreStat(userId, scoreStat.getSumScore(), scoreStat.getCountScore(), scoreStat.getAvgScore(), windowEnd));
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class UserScoreTs {
        private String userId;
        private Integer Score;
        private Long ts;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class ScoreStat {
        private Long sumScore;
        private Long countScore;
        private Double avgScore;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class UserScoreStat {
        private String userId;
        private Long sumScore;
        private Long countScore;
        private Double avgScore;
        private Long windowEnd;
    }
}
