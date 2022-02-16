package com.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 利用KeyedState实现sum功能,报错以后从上一个Checkpoint恢复状态进行重启
 * 效果同CheckpointRestartDemo
 */
public class RestartFromKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //开启checkpoint
        env.enableCheckpointing(5000); //开启checkpoint，默认的重启策略就是无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        // word1
        // word2
        // error
        // ...
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.equals("error")) {
//                    int i = 1 / 0; //模拟出现错误，任务重启
                    throw new RuntimeException("任务报错,开始重启...");
                }
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> countState;

            //在构造器方法之后，map方法之前执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态或恢复状态
                //使用状态的步骤：
                //1.定义一个状态描述器，状态的名称，存储数据的类型等
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(
                        "wc-state",
                        Integer.class
                );
                //2.使用状态描述从对应的StateBack器获取状态
                countState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String currentKey = input.f0;
                Integer currentCount = input.f1;
                Integer historyCount = countState.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + currentCount;
                //更新state
                countState.update(sum);
                return Tuple2.of(currentKey, sum);
            }
        });

        result.print();

        env.execute();
    }
}