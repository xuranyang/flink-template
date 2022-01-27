package com.demo;

import com.google.common.collect.Lists;
import com.model.UserPos;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 假设club有LGD EHOME Elephant 3种,
 * 若根据club进行keyby,且并行度为2,同一个club的数据一定是会在一个并行度里面(不可能会在多个并行度里同时出现)
 * 但只做 keyby 的话,可以看到,第1个并行度里为LGD,第2个并行度里为EHOME、Elephant(说明只做keyby的话,一个并行度里面是有可能会出现多个club的)
 * 如果 keyby+window 的话,就可以保证同一个窗口内一定只有一个club,不会出现一个窗口内有多个club的情况
 */
public class KeybyWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(2);

        DataStream<String> dataStream = env.readTextFile("flink-template-core/src/main/java/com/data/data2.txt");

        dataStream.map(s -> {
            String[] split = s.split(",");
            return new UserPos(split[0], split[1], split[2]);
        })
        .keyBy(new KeySelector<UserPos, String>() {
            @Override
            public String getKey(UserPos userPos) throws Exception {
                return userPos.getClub();
            }
        })
        .countWindow(2).apply(new WindowFunction<UserPos, List<UserPos>, String, GlobalWindow>() {
            @Override
            public void apply(String s, GlobalWindow window, Iterable<UserPos> input, Collector<List<UserPos>> out) throws Exception {
                List<UserPos> userList = Lists.newArrayList(input.iterator());
//                System.out.println(userList);
                out.collect(userList);
            }
        })
        .print();

        env.execute();
    }
}
