package com.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink1.12 不再使用split 和 select
 * 使用process 和 OutputTag
 * 根据奇偶性
 * 将数据流拆分成多条
 */
public class SplitDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        // 奇数
        OutputTag<Integer> oddTag = new OutputTag<>("odd", TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<>("even", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> process = dataStream.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {

                if (value % 2 == 1) {
                    ctx.output(oddTag, value);
                } else {
                    ctx.output(evenTag, value);
                }

            }
        });

//        System.out.println(oddTag);
//        System.out.println(evenTag);

        DataStream<Integer> oddOutput = process.getSideOutput(oddTag);
        DataStream<Integer> evenOutput = process.getSideOutput(evenTag);

        oddOutput.print("odd");

        evenOutput.print("even");

        env.execute();


    }
}
