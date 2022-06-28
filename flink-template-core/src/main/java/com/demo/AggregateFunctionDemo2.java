package com.demo;

import com.model.UserBehavior;
import com.model.UserItemWindowAgg;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregateFunctionDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(30000);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                String userId = split[0];
                Long score = Long.valueOf(split[1]);
                return Tuple2.of(userId, score);
            }
        }).keyBy(0).timeWindow(Time.seconds(10))
                .aggregate(new incrAggAvgByUserId(), new windowIncrAggAvgResult()).print();
        env.execute();
    }

    public void eventTimeAgg(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource = env.readTextFile("flink-template-core/src/main/java/com/data/agg_data2.txt");
        DataStream<String> dataStream = dataStreamSource.filter(s -> !s.startsWith("-")).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.valueOf(element.split(",")[2]);
            }
        });

        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                String userId = split[0];
                Long score = Long.valueOf(split[1]);
                return Tuple2.of(userId, score);
            }
        }).keyBy(0).timeWindow(Time.seconds(3))
                .aggregate(new incrAggAvgByUserId(), new windowIncrAggAvgResult()).print();
    }

    public static class incrAggAvgByUserId implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Long> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            Long score = value.f1;
            System.out.println("[Add]" + value + "|" + Tuple2.of(accumulator.f0 + 1, accumulator.f1 + score));
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + score);
        }

        @Override
        public Long getResult(Tuple2<Long, Long> accumulator) {
            return accumulator.f1 / accumulator.f0;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    public static class windowIncrAggAvgResult implements WindowFunction<Long, Tuple3<String, Long, Long>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            String userId = key.toString();
            Long windowEnd = window.getEnd();
            Long avgScore = input.iterator().next();
            out.collect(Tuple3.of(userId, avgScore, windowEnd));
        }
    }
}
