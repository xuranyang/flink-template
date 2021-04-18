package com.sql.new_version;

import com.model.PlayerInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        /**
         * 可以显示指定使用新版本blink执行计划 或者 老版本执行计划
         */
        /*基于Blink的流处理*/
//        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        /*基于Blink的批处理*/
//        EnvironmentSettings newBatchSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        /*基于老版本planner的流处理*/
//        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        /*基于老版本planner的批处理*/
//        EnvironmentSettings oldBathSettings = EnvironmentSettings.newInstance().useOldPlanner().inBatchMode().build();

        /**
         * 创建Table流环境
         */
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,newStreamSettings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputDataStream = env.readTextFile("src/main/java/com/data/data2.txt");

        DataStream<PlayerInfo> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new PlayerInfo(fields[0], fields[1], fields[2], fields[3]);
        });

        /**
         *  DataStream -> Table API
         */
        // 如果DataStream中的数据类型是POJO类,默认会根据POJO类获得字段名称
        Table table = tableEnv.fromDataStream(dataStream);
        // 手动绑定
//        Table table = tableEnv.fromDataStream(dataStream, $("club"), $("userId"), $("pos"), $("age"));


//        table.printSchema();

        Table resultTable = table.select("club,userId,pos,age").where("club='LGD'");
//        Table resultTable = table.groupBy("club").select("club,count(1) as cnt");
        /**
         * Table API -> DataSteam
         */
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
//        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(resultTable, Row.class);
        rowDataStream.print("table-api");

        /**
         *  Table API -> SQL
         */
        tableEnv.createTemporaryView("info", table);
//        String sql = "select club,userId,pos,age from info where club='LGD'";
        String sql = "select club,count(1) as cnt from info group by club";
        Table sqlQueryTable = tableEnv.sqlQuery(sql);
//        DataStream<Row> sqlDataSteam = tableEnv.toAppendStream(sqlQueryTable, Row.class);


//        sqlQueryTable.printSchema();
        /**
         * 使用 group by、distinct 等方法时,会涉及到更新操作,必须使用toRetractStream
         */
        DataStream<Tuple2<Boolean, Row>> sqlDataSteam = tableEnv.toRetractStream(sqlQueryTable, Row.class);
        sqlDataSteam.print("sql");


        env.execute();
    }
}
