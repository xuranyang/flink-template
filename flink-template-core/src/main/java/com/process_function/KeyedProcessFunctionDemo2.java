package com.process_function;


import com.model.UserApm;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 统计选手 APM连续5秒增长
 */
public class KeyedProcessFunctionDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<String> dataStream = env.socketTextStream("localhost", 1234);
//        DataStream<String> dataStream = env.readTextFile("flink-template-core/src/main/java/com/data/data3.txt");

        dataStream.map(line -> {
            String[] split = line.split(",");
            String userId = split[0];
            Integer apm = Integer.parseInt(split[1]);
            return new UserApm(userId, apm);
        }).keyBy(new KeySelector<UserApm, String>() {
            @Override
            public String getKey(UserApm userApm) throws Exception {
                return userApm.getUserId();
            }
        }).process(new ApmContinueIncr(5)).print();

        env.execute();
    }

    //KEY IN OUT
    public static class ApmContinueIncr extends KeyedProcessFunction<String, UserApm, String> {

        // 时间间隔
        private Integer interval;
        private ValueState<Integer> lastApmState;
        private ValueState<Long> timerTsState;

        public ApmContinueIncr(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastApmState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-apm", Integer.class, Integer.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(UserApm userApm, Context context, Collector<String> out) throws Exception {
            // 取出保存的状态
            Integer lastApm = lastApmState.value();
            Long timerTs = timerTsState.value();
            Integer apm = userApm.getApm();
            // 还没有定时器,如果apm上升
            if (apm > lastApm && timerTs == null) {
                Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                // 如果定时器已经存在,不会重复注册
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果apm下降,且已经注册了定时器
            else if (apm < lastApm && timerTs != null) {
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            // 更新最新的apm
            lastApmState.update(apm);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("触发定时器UserId:" + ctx.getCurrentKey());
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastApmState.clear();
        }

    }
}
