package com.trigger;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 数量窗口增加最大等待时间限制
 * 等价于时间窗口 增加最大数量限制 的触发器
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

        // 窗口内数据达到3条 或者 窗口到达5秒 就触发此窗口
        dataStreamSource.countWindowAll(3)
                .trigger(PurgingTrigger.of(CountWithMaxTimeTrigger.of(3, 5000)))
                .apply(new AllWindowFunction<String, List<String>, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
                        List<String> valuesList = Lists.newArrayList(values.iterator());
                        out.collect(valuesList);
                    }
                }).print();


        env.execute();
    }
}
