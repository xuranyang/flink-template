package com.demo;

import com.sink.MySqlTwoPhaseCommitSink;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/*
insert into user_info(user_id,name,age) values(1,'aaa',19)
insert into user_info(user_id,name,age) values(2,'bbb',20)
insert into user_info(user_id,name,age) values(3,'ccc',21)

insert into user_info(user_id,name,age) values(4,'ddd',22)
insert into user_info(user_id,name,age) values(5,'eee',23)
insert into user_info(user_id,name,age) values(1,'fff',24)  # 故意让主键冲突报错

insert into user_info(user_id,name,age) values(7,'ggg',30)
*/
public class MySqlTwoPhaseCommitDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 1000L));
        env.setParallelism(1);

        // nc -lp 8888
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        env.enableCheckpointing(30000);

        SingleOutputStreamOperator<List<String>> dataStream = dataStreamSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))).process(new ProcessAllWindowFunction<String, List<String>, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<String, List<String>, TimeWindow>.Context context, Iterable<String> iterable, Collector<List<String>> out) throws Exception {
                List<String> sqlList = Lists.newArrayList(iterable.iterator());
                out.collect(sqlList);
                System.out.println("============ Windiw Processed ============");
            }
        });

        dataStream.addSink(new MySqlTwoPhaseCommitSink());

        env.execute("MySQL 2PC Demo");
    }
}
