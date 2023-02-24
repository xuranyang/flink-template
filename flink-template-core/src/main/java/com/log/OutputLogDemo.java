package com.log;

import com.source.RandomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputLogDemo {
    private static Logger log = LoggerFactory.getLogger(OutputLogDemo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new RandomSource(200));

        dataStreamSource.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<String> collector) throws Exception {
                log.info("[OutputLog]:{}", value);
                collector.collect(value.toString());
            }
        }).print();

        env.execute();
    }
}
