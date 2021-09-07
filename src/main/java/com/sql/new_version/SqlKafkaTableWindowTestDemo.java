package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlKafkaTableWindowTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);


        /**
         * {
         *   "age": "11",
         *   "id": "1234",
         *   "ts": 1630980584233
         * }
         */
        // TIMESTAMP 类型只支持10位的时间戳
        // -- ts 为13位的时间戳,需要做一下转化
        // -- 在process_time上定义1 秒延迟的 watermark
        // DDL中不能字段名不能出现timestamp等关键字
        // 窗口区间 左闭右开 [27,30) [30,33)
        tableEnv.executeSql("CREATE TABLE flink_test_table2(" +
                "id STRING, " +
                "age STRING," +
                "ts BIGINT," +
//                "process_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "process_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000)), " +
                "WATERMARK FOR process_time AS process_time - INTERVAL '1' SECOND" +
                ")" +
                "WITH" +
                "(" +
                "   'connector.type' = 'kafka'," +
                "   'connector.version' = 'universal'," +
                "   'connector.topic' = 'KAFKA_TOPIC_NAME_2'," +
                "   'connector.startup-mode' = 'latest-offset'," +
                "   'connector.properties.zookeeper.connect' = '127.0.0.1:2181'," +
                "   'connector.properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "   'connector.properties.group.id' = 'flink_sql_test_group_id'," +
                "   'format.type' = 'json'," +
                "   'format.derive-schema' = 'true'," +
                "   'update-mode' = 'append'" +
                ")");

//        String querySql = "SELECT * FROM flink_test_table2";
//        String querySql = "SELECT id,age,ts,process_time FROM flink_test_table2";

//        String querySql = "SELECT tumble_rowtime(process_time, interval '3' seconds)," +
//                "tumble_start(process_time, interval '3' second)," +
//                "tumble_end(process_time, interval '3' second)," +
//                "count(1) " +
//                "FROM flink_test_table2 group by tumble(process_time,interval '3' second)";

        String querySql = "SELECT tumble_rowtime(process_time, interval '3' seconds)," +
                "tumble_start(process_time, interval '3' second)," +
                "tumble_end(process_time, interval '3' second)," +
                "id,age " +
                "FROM flink_test_table2 group by tumble(process_time,interval '3' second),id,age";
        Table table = tableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("[KafkaWindowInfo]");

    }
}
