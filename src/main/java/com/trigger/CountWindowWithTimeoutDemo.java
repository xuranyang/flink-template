package com.trigger;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 时间窗口 增加最大数量限制 的触发器
 * 等价于数量窗口增加最大等待时间限制
 */
public class CountWindowWithTimeoutDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(600000);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // nc -l -p 8888
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);

//        dataStreamSource.print();

        // 窗口到达10秒 或者 窗口内数据达到3条 就触发此窗口
        dataStreamSource.timeWindowAll(Time.seconds(10)).trigger(new CountTriggerWithTimeout(3, TimeCharacteristic.ProcessingTime))
                .apply(new AllWindowFunction<String, List<String>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
//                System.out.println(values);
                        List<String> valuesList = Lists.newArrayList(values.iterator());
                        out.collect(valuesList);
                    }
                }).print();

        env.execute();
    }
}
