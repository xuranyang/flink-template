package com.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * https://www.jianshu.com/p/de28474fdb85
 * https://blog.csdn.net/lmalds/article/details/53736836
 * <p>
 * slot1:mapDs,mapDs2,filterDs
 * slot2:flatMapDs
 * slot3:dataStreamSource
 */
public class SlotSharingGroupDemo {
    private static final String slotGroupName1 = "slot-group-1";
    private static final String slotGroupName2 = "slot-group-2";
    private static final String slotGroupName3 = "slot-group-3";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // http://localhost:18081/
        configuration.setInteger(RestOptions.PORT, 18081);
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        configuration.setDouble(TaskManagerOptions.CPU_CORES, 3.0);
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("256m"));
//        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("64m"));
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("128m"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(60000L);

        // nc -lp8888
        SingleOutputStreamOperator<String> dataStreamSource = env.socketTextStream("localhost", 8888)
                .slotSharingGroup(slotGroupName3);

        DataStream<String> mapDs = dataStreamSource.map(s -> "[Map Info]:" + s)
                .uid("map-ds").name("map-ds").slotSharingGroup(slotGroupName1);

        DataStream<String> mapDs2 = dataStreamSource.map(s -> "[Map Info2]:" + s)
                .uid("map-ds2").name("map-ds").slotSharingGroup(slotGroupName1);

        DataStream<String> filterDs = dataStreamSource.filter(s -> !s.startsWith("1"))
                .uid("filter-ds").name("filter-ds").slotSharingGroup(slotGroupName1);

        DataStream<String> flatMapDs = dataStreamSource.flatMap((FlatMapFunction<String, String>) (s, out) -> {
                    out.collect("[FlatMap Message 1]:" + s);
                    out.collect("[FlatMap Message 2]:" + s);
                }).returns(TypeInformation.of(String.class))
                .uid("flatMap-ds").name("flatMap-ds").slotSharingGroup(slotGroupName2);

        mapDs.print("Map");
        mapDs2.print("Map2");
        filterDs.print("Filter");
        flatMapDs.print("FlatMap");

        env.execute();
    }
}
