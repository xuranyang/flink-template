package com.state;

import com.source.RandomSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

/**
 * Transformation属性:
 * uid : 用户指定的uid，该uid的主要目的是用于在job重启时可以再次分配跟之前相同的uid，应该是用于持久保存状态的目的。
 * name : 转换器的名称，这个主要用于WebUI可视化的目的
 * parallelism : 并行度
 * bufferTimeout ：buffer超时时间
 * id : 跟属性uid无关，它的生成方式是基于一个静态累加器
 * outputType ： 输出类型
 * slotSharingGroup : 给当前的transformation设置slot共享组。
 * slot sharing group用于将并行执行的operator“归拢”到相同的TaskManager slot中
 * （slot概念基于资源的划分，因此这里的目的是让不同的subtask共享slot资源）
 */

//https://blog.csdn.net/wangpei1949/article/details/100608828
public class UIdDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStateBackend(new MemoryStateBackend());
        // Flink1.13以后 HashMapStateBackend+JobManagerStateBackend 等价于 MemoryStateBackend
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerStateBackend());

//        env.enableCheckpointing(10000);

        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new RandomSource(5000));

//        dataStreamSource.print();

        boolean disableChaining = true;
//        boolean disableChaining = false;
        if (disableChaining) {
            // 使用disableChaining
            dataStreamSource.disableChaining()
                    .map(element -> element.f0 + "-" + element.f1).uid("str-map").name("str-map").disableChaining()
                    .map(new operatorStateMapFunction()).uid("op-state-map").name("op-state-map").disableChaining()
                    .print().uid("res-print").name("res-print");
        } else {
            // 不使用disableChaining
            dataStreamSource.map(element -> element.f0 + "-" + element.f1).uid("str-map").name("str-map")
                    .map(new operatorStateMapFunction()).uid("op-state-map").name("op-state-map")
                    .print().uid("res-print").name("res-print");
        }

        env.execute();
    }

    public static class operatorStateMapFunction extends RichMapFunction<String, String> implements CheckpointedFunction {

        private ListState<String> listState;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> opListStateDescriptor = new ListStateDescriptor<>("opListStateDescriptor", String.class);
            listState = context.getOperatorStateStore().getListState(opListStateDescriptor);
            listState.update(Collections.emptyList());
        }

        @Override
        public String map(String value) throws Exception {
            listState.add(value + "|" + sdf.format(new Date()));
//            System.out.println(listState.get());
            return "[ListState]->" + listState.get().toString();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

    }
}
