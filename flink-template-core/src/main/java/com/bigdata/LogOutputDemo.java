package com.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://www.cnblogs.com/upupfeng/p/14489664.html
 */
public class LogOutputDemo {
    private static Logger log = LoggerFactory.getLogger(LogOutputDemo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(1, 2, 3, 4, 5, -1).flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> collector) throws Exception {
                if (value > 0) {
                    log.info("[Flink-Map][INFO]:{}", value);
                    collector.collect(value * 2);
                } else {
                    log.error("[Flink-Map][ERROR]:{}", value);
                }
            }
        });

        env.execute();
    }
}
