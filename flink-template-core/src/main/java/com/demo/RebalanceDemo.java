package com.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * 自定义分区方法，实现模拟数据倾斜
 * 使用rebalance等方法实现重分区
 * keyBy()会根据HashCode进行重分区
 */
public class RebalanceDemo implements Serializable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 这种方式进行批处理还是会存在一些问题,比如聚合时只能使用keyby,不能使用groupby,这会导致:
         * 使用sum算子时,只有超过2次的word才会被打印出来,只出现1次的数据不会被打印
         */
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
//        根据数据源自动选择使用流还是批
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


//        System.out.println(System.getProperty("user.dir"));
        DataStream<String> dataStream = env.readTextFile("flink-template-core/src/main/java/com/data/data.txt");


        SingleOutputStreamOperator<Tuple2<String, Integer>> wc = dataStream.flatMap(new FlatMapFunction<String, String>() {
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
        });

        DataStream<Tuple2<String, Integer>> result = wc.partitionCustom(new UserDefinePartitioner(), t -> t.f0);

//        result.print("default");
        result.rebalance().print("rebalance");
//        result.shuffle().print("shuffle");
//        result.broadcast().print("broadcast");
//        result.forward().print("forward");
//        result.rescale().print("rescale");

        env.execute();

    }


    public static class UserDefinePartitioner implements Partitioner<String> {
        @Override
        public int partition(String s, int i) {
            if (s != null) {
                return 0;
            } else {
                return 1;
            }
        }
    }


}
