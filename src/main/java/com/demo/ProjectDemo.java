package com.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        List<Tuple3<String, String, Integer>> tuple3List = Lists.newArrayList();
        tuple3List.add(Tuple3.of("maybe", "rng", 26));
        tuple3List.add(Tuple3.of("chalice", "rng", 22));
        tuple3List.add(Tuple3.of("yatoro", "spirit", 20));
        tuple3List.add(Tuple3.of("collapse", "spirit", 20));
        tuple3List.add(Tuple3.of("ana", "og", 21));

        DataStreamSource<Tuple3<String, String, Integer>> dataStream = env.fromCollection(tuple3List);

        dataStream.project(2, 0).print();

        env.execute();

    }
}
