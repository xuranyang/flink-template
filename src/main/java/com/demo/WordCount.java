package com.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.Serializable;


public class WordCount implements Serializable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 这种方式进行批处理还是会存在一些问题,比如聚合时只能使用keyby,不能使用groupby,这会导致:
         * 使用sum算子时,只有超过2次的word才会被打印出来,只出现1次的数据不会被打印
         */
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


//        System.out.println(System.getProperty("user.dir"));
        DataStream<String> dataStream = env.readTextFile("src/main/java/com/data/data.txt");


        dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {

                String[] words = s.split(" ");

                for (String word : words) {
                    out.collect(word);
                }

            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        }).keyBy(wc -> wc.f0).sum(1).print();

        env.execute();

    }

//    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
//    public static class Wc {
//        private String word;
//        private Integer count;
//    }


}
