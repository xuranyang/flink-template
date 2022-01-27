package com.state;

import com.model.UserPos;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.function.RichProcessWindowFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed State:
 * ValueState
 * ListState
 * ReducingState
 * AggregatingState
 * MapState
 */
public class KeyedStateDemo {
    /**
     * 求当前流中,各个分区最小的pos及其id
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.readTextFile("flink-template-core/src/main/java/com/data/data2.txt");

        dataStream.map(new MapFunction<String, UserPos>() {
            @Override
            public UserPos map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserPos(split[0], split[1], split[2]);
            }
        }).keyBy(new KeySelector<UserPos, String>() {
            @Override
            public String getKey(UserPos userPos) throws Exception {
                return userPos.getClub();
            }
        }).map(new UserDefineMapFunction()).print();

        env.execute();
    }


    public static class UserDefineMapFunction extends RichMapFunction<UserPos, String> {
        private ValueState<String> minValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("minValueState", String.class, null);
            /**
             * 获取KeyedState
             */
            minValueState = getRuntimeContext().getState(valueStateDescriptor);
        }


        @Override
        public String map(UserPos userPos) throws Exception {
            String value = minValueState.value();

            if (value == null) {
                String currentValue = userPos.getUserId() + "|" + userPos.getPos();
                minValueState.update(currentValue);
            } else {


                Integer statePos = Integer.parseInt(value.split("\\|")[1]);
                Integer currentPos = Integer.parseInt(userPos.getPos());

                if (currentPos < statePos) {
                    String currentValue = userPos.getUserId() + "|" + userPos.getPos();
                    minValueState.update(currentValue);
                }

            }
            return minValueState.value();
        }
    }
}
