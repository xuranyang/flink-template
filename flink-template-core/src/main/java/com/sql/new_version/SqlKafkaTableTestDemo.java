package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlKafkaTableTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        /**
         * {
         *   "name": "Maybe"
         * }
         */
        // executeSql or sqlUpdate
        tableEnv.executeSql("CREATE TABLE flink_test_table(" +
                "name STRING" +
                ")" +
                "WITH" +
                "(" +
                "   'connector.type' = 'kafka'," +
                "   'connector.version' = 'universal'," +
                "   'connector.topic' = 'KAFKA_TOPIC_NAME'," +
                "   'connector.startup-mode' = 'earliest-offset'," +
                "   'connector.properties.zookeeper.connect' = '127.0.0.1:2181'," +
                "   'connector.properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "   'connector.properties.group.id' = 'flink_sql_test_group_id'," +
                "   'format.type' = 'json'," +
                "   'format.derive-schema' = 'true'," +
                "   'update-mode' = 'append'" +
                ")");

        String querySql = "SELECT name FROM flink_test_table";
        Table table = tableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("[KafkaInfo]");

    }
}
