package com.demo;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Time;
import java.util.List;
import java.util.concurrent.TimeUnit;

//Iterate迭代算子会不断重复循环,直到符合要求为止
public class IterateDemo {
    static int sleepSencods;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        List<Tuple3<String, String, Integer>> tuple3List = Lists.newArrayList();
        tuple3List.add(Tuple3.of("maybe", "rng", 26));
        tuple3List.add(Tuple3.of("chalice", "rng", 22));
        tuple3List.add(Tuple3.of("yatoro", "spirit", 20));
        tuple3List.add(Tuple3.of("collapse", "spirit", 20));
        tuple3List.add(Tuple3.of("ana", "og", 21));

        DataStreamSource<Tuple3<String, String, Integer>> dataStream = env.fromCollection(tuple3List);

        // 最长等待5秒,如果流一直无限迭代,超过5秒就会停止迭代
        IterativeStream<Tuple3<String, String, Integer>> iterateDataStream = dataStream.iterate(5000);

        boolean ifSleep = true;

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> feedback = iterateDataStream
                .map(new MapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> tuple3) throws Exception {

                        if (ifSleep) {
                            /**
                             * 设置最长等待时间为 5秒
                             * 如果每次休眠2秒,最长一次等待时间是4秒:
                             * maybe,rng,26(不满足<25的过滤条件,直接跳过) + [2s] + chalice,rng,22 + [2s] -> chalice,rng,23
                             * 4秒 < 5秒,所以可以一直循环迭代输出[IterateResult]信息,直到不满足过滤条件为止
                             *
                             * 如果每次休眠3秒,最长一次等待时间是6秒:
                             * maybe,rng,26(不满足<25的过滤条件,直接跳过) + [3s] + chalice,rng,22 + [3s] -> chalice,rng,23
                             * 6秒 > 5秒,超过了最大等待时间,直接停止迭代,不会输出[IterateResult]信息
                             */
                            sleepSencods = 2;
//                            sleepSencods = 3;
                            TimeUnit.SECONDS.sleep(sleepSencods);

                            System.out.println("[SleepSencods]:" + sleepSencods + "|" + "[Data]:" + tuple3);
                        }
                        return Tuple3.of(tuple3.f0, tuple3.f1, tuple3.f2 + 1);
                    }
                })
                .filter(new FilterFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> tuple3) throws Exception {
                        return tuple3.f2 < 25;
                    }
                });

        iterateDataStream.closeWith(feedback);

        iterateDataStream.map(new MapFunction<Tuple3<String, String, Integer>, String>() {
            @Override
            public String map(Tuple3<String, String, Integer> tuple3) throws Exception {
                return "[IterateResult]:" + tuple3;
            }
        }).print();

        env.execute();
    }
}
