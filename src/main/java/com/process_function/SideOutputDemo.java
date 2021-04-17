package com.process_function;

import com.model.UserApm;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> dataStream = env.readTextFile("src/main/java/com/data/data3.txt");

        /**
         * 定义侧输出流
         */
        OutputTag<UserApm> lowApmOutputTag = new OutputTag<UserApm>("low-apm"){};
        SingleOutputStreamOperator<UserApm> highApmDataStream = dataStream.map(line -> {
            String[] split = line.split(",");
            String userId = split[0];
            Integer apm = Integer.parseInt(split[1]);
            return new UserApm(userId, apm);
        }).process(new ProcessFunction<UserApm, UserApm>() {
            @Override
            public void processElement(UserApm userApm, Context context, Collector<UserApm> out) throws Exception {
                if (userApm.getApm() <= 100) {
                    context.output(lowApmOutputTag, userApm);
                } else {
                    out.collect(userApm);
                }
            }
        });

        highApmDataStream.print("high-apm");
        highApmDataStream.getSideOutput(lowApmOutputTag).print("low-apm");

        env.execute();
    }
}
