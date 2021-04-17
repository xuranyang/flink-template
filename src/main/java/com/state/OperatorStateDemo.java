package com.state;

import com.model.UserPos;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Operator State:只有ListState
 */
public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.readTextFile("src/main/java/com/data/data2.txt");
        dataStream.map(new UserDefineOperatorStateFunction()).print();

        env.execute();
    }

    /**
     * 获取当前所有选手中,年龄最大的选手信息
     */
    public static class UserDefineOperatorStateFunction extends RichMapFunction<String, String> implements CheckpointedFunction {

        private ListState<String> listState;


        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            ListStateDescriptor<String> myListStateDescriptor = new ListStateDescriptor<>("myListStateDescriptor", String.class);
            /**
             * 获取 Operator State
             */
            listState = ctx.getOperatorStateStore().getListState(myListStateDescriptor);
            listState.update(Arrays.asList("init"));
        }

        @Override
        public String map(String s) throws Exception {
            String[] split = s.split(",");
            String userId = split[1];
            int currentAge = Integer.parseInt(split[3]);
            if (listState.get().iterator().next().equals("init")) {
                listState.update(Arrays.asList(userId + "|" + currentAge));
            } else {
                Iterable<String> iterable = listState.get();
                for (String element : iterable) {
                    int stateAge = Integer.parseInt(element.split("\\|")[1]);
                    if (currentAge > stateAge) {
                        listState.update(Arrays.asList(userId + "|" + currentAge));
                    }

                }
            }
            return listState.get().iterator().next();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        }

    }
}
