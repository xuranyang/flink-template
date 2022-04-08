package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlUpsertKafakaDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        /**
         * Source Kafka Data Example:
         * {
         *   "name": "maybe",
         *   "amount": 19
         * }
         */
        // executeSql or sqlUpdate
        String sourceSql = "CREATE TABLE kakfa_source_table(" +
                "name STRING," +
                "amount INT" +
                ")" +
                "WITH" +
                "(" +
                "   'connector.type' = 'kafka'," +
                "   'connector.version' = 'universal'," +
                "   'connector.topic' = 'source_topic_1'," +
                "   'connector.startup-mode' = 'earliest-offset'," +
                "   'connector.properties.zookeeper.connect' = '127.0.0.1:2181'," +
                "   'connector.properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "   'connector.properties.group.id' = 'flink_sql_kafka_test_group_id'," +
                "   'format.type' = 'json'," +
                "   'format.derive-schema' = 'true'," +
                "   'update-mode' = 'append'" +
                ")";

        String sinkSql = "CREATE TABLE kafka_sink_table (\n" +
                " name STRING," +
                " sum_amount INT,\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                " ) WITH (\n" +
                " 'connector' = 'upsert-kafka',\n" +
                " 'topic' = 'sink_topic_2',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'key.json.ignore-parse-errors' = 'true',\n" +
                " 'value.json.fail-on-missing-field' = 'false',\n" +
                " 'key.format' = 'json',\n" +
                " 'value.format' = 'json'\n" +
                " )";
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);

        String insertSql = "INSERT INTO kafka_sink_table SELECT name,sum(amount) FROM kakfa_source_table group by name";
        tableEnv.executeSql(insertSql);

        String querySql = "SELECT name,sum_amount FROM kafka_sink_table";
        tableEnv.executeSql(querySql).print();

//        Table table = tableEnv.sqlQuery(querySql);
//        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(table, Row.class);
//        rowDataStream.print();

        env.execute("[Kafka Real Time]");
    }
}
