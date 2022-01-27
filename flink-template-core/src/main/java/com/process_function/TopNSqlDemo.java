package com.process_function;

import com.model.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TopNSqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputDataStream = env.readTextFile("flink-template-core/src/main/java/com/data/timer_data.txt");

        DataStream<UserBehavior> dataStream = inputDataStream.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                // 处理掉 "-" 开头的脏数据
                if (!value.startsWith("-")) {
                    String[] fields = value.split(",");
                    out.collect(UserBehavior.builder()
                            .userId(fields[0])
                            .item(fields[1])
                            .timestamp(Long.valueOf(fields[2]))
                            .build());
                }
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp();
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Table dataTable = tableEnv.fromDataStream(dataStream, "userId,item,timestamp.rowtime as ts");

        Table windowAggTable = dataTable
                .filter("item = 'bkb'")
                .window(Slide.over("20.seconds").every("10.seconds").on("ts").as("w"))
                .groupBy("item, w")
                .select("item, w.end as windowEnd, item.count as cnt");

        // 利用开窗函数，对count值进行排序并获取ROW_NUMBER()，得到TopN
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "item, windowEnd, cnt");

        Table resultTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from agg) " +
                " where row_num <= 5 ");

//        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "userId,item,timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select item, count(item) as cnt, HOP_END(ts, interval '10' second, interval '20' second) as windowEnd " +
                "    from data_table " +
                "    where item = 'bkb' " +
                "    group by item, HOP(ts, interval '10' second, interval '20' second)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute();

    }
}
