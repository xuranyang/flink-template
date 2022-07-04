package com.demo;


import com.google.common.collect.Lists;
import com.model.UserScore;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class DataSkewDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        int parallelism = 3;
        env.setParallelism(parallelism);

        DataStream<UserScore> dataStream = env.fromElements(
                new UserScore("Ame", 100),
                new UserScore("Ame", 200),
                new UserScore("Burning", 10),
                new UserScore("Burning", 20),
                new UserScore("Burning", 30),
                new UserScore("Burning", 40),
                new UserScore("Burning", 50),
                new UserScore("Burning", 60),
                new UserScore("Burning", 70),
                new UserScore("Burning", 80),
                new UserScore("Chalice", 90),
                new UserScore("Dy", 80),
                new UserScore("Eurus", 100)
        );

        KeyedStream<UserScore, String> skewDataStream = dataStream.keyBy(UserScore::getUserName);
//        skewDataStream.print("数据倾斜");
        skewDataStream.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum("userScore").print("窗口数据倾斜");

        skewDataStream.keyBy(new KeySelector<UserScore, Integer>() {
            @Override
            public Integer getKey(UserScore value) throws Exception {
                return new Random().nextInt(Integer.MAX_VALUE);
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply(new WindowFunction<UserScore, UserScore, Integer, TimeWindow>() {
            @Override
            public void apply(Integer integer, TimeWindow window, Iterable<UserScore> input, Collector<UserScore> out) throws Exception {
                List<UserScore> userScores = Lists.newArrayList(input);
                Map<String, Integer> userSumScoreMap = userScores.stream().collect(Collectors.groupingBy(UserScore::getUserName, Collectors.summingInt(UserScore::getUserScore)));
                userSumScoreMap.forEach((k, v) -> {
                    out.collect(new UserScore(k,v));
                });
            }
        }).keyBy(UserScore::getUserName)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum("userScore").print("解决窗口数据倾斜");

        env.execute();
    }
}
