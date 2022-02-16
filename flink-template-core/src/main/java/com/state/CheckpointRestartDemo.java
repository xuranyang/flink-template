package com.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * a -> (a,1)
 * b -> (b,1)
 * a -> (a,2)
 * b -> (b,2)
 * a -> (a,3)
 * restart -> 报错重启
 * --
 * a -> checkpoint完成以后:(a,4) | checkpoint还未完成 或 不使用checkpoint:(a,1)
 * b -> checkpoint完成以后:(b,3) | checkpoint还未完成 或 不使用checkpoint:(b,1)
 */
public class CheckpointRestartDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        boolean useCkp = true;
//        boolean useCkp = false;
        if (useCkp) {
            env.setStateBackend(new MemoryStateBackend());
            // Flink1.13以后 HashMapStateBackend+JobManagerStateBackend 等价于 MemoryStateBackend
            // env.setStateBackend(new HashMapStateBackend());
            // env.getCheckpointConfig().setCheckpointStorage(new JobManagerStateBackend());
            env.enableCheckpointing(20000);
        }

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.equals("restart")) {
//                    int i = 1 / 0;
                    throw new RuntimeException("任务报错,开始重启...");
                }
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum(1);

        result.print();
        env.execute();
    }
}
