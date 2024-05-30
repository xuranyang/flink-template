package com.util.mock;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Mock造数据 Flink Source Demo
 */
public class MockFlinkSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<Integer> mockSource = new AbstractMockFlinkSource<Integer>(1000L) {
            /**
             * 每间隔 1000ms 生产一条数据
             */
            int num = 0;

            @Override
            void mockData(SourceContext<Integer> ctx, long sleepMs) {
                ctx.collect(num++);
            }
        };

        DataStreamSource<Integer> dataStreamSource = env.addSource(mockSource);
        dataStreamSource.print("MockData");
        env.execute();
    }
}
