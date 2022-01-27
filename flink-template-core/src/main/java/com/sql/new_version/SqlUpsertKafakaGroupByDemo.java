package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlUpsertKafakaGroupByDemo {
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
                "   'connector.topic' = 'canal_test_2'," +
//                "   'connector.startup-mode' = 'earliest-offset'," +
                "   'connector.startup-mode' = 'latest-offset'," +
                "   'connector.properties.zookeeper.connect' = '127.0.0.1:2181'," +
                "   'connector.properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "   'connector.properties.group.id' = 'flink_sql_kafka_test_group_id'," +
                "   'format.type' = 'json'," +
                "   'format.derive-schema' = 'true'," +
                "   'update-mode' = 'append'" +
                ")";

        String sinkSql = "CREATE TABLE kafka_sink_table (\n" +
                " name STRING,\n" +
                " sum_amount INT,\n" +
                " cur_time TIMESTAMP(3),\n" +
                " PRIMARY KEY (name) NOT ENFORCED\n" +
                " ) WITH (\n" +
                " 'connector' = 'upsert-kafka',\n" +
                " 'topic' = 'canal_test_3',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'key.format' = 'json',\n" +
                " 'value.fields-include' = 'EXCEPT_KEY',\n" +
                " 'value.format' = 'json'\n" +
                " )";
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);

        String insertSql = "INSERT INTO kafka_sink_table SELECT name,sum(amount),CURRENT_TIMESTAMP AS cur_time FROM kakfa_source_table group by name";
        tableEnv.executeSql(insertSql);

        /**
         * Kafka Key:
         * {
         *   "name": "maybe"
         * }
         *
         * Kafka Value:
         * {
         *   "sum_amount": 78,
         *   "cur_time": "2021-10-29 07:56:51.103"
         * }
         */
        //        String querySql = "SELECT name,sum_amount FROM (SELECT name,sum_amount,ROW_NUMBER() OVER (PARTITION BY name ORDER BY cur_time desc) as rnk FROM kafka_sink_table) t WHERE rnk=1";
        String querySql = "SELECT name,sum_amount,cur_time FROM kafka_sink_table";
        tableEnv.executeSql(querySql).print();

//        String sourceQuerySql = "CREATE TABLE kakfa_source_query_table(" +
//                " name STRING," +
//                " sum_amount INT" +
//                ")" +
//                "WITH" +
//                "(" +
//                "   'connector.type' = 'kafka'," +
//                "   'connector.version' = 'universal'," +
//                "   'connector.topic' = 'canal_test_3'," +
//                "   'connector.startup-mode' = 'earliest-offset'," +
//                "   'connector.properties.zookeeper.connect' = '127.0.0.1:2181'," +
//                "   'connector.properties.bootstrap.servers' = '127.0.0.1:9092'," +
//                "   'connector.properties.group.id' = 'flink_sql_kafka_test_group_id'," +
//                "   'format.type' = 'json'," +
//                "   'format.derive-schema' = 'true'," +
//                "   'update-mode' = 'append'" +
//                ")";
//
//        tableEnv.executeSql(sourceQuerySql);
//        String querySql = "SELECT name,sum_amount FROM kakfa_source_query_table";
//
//
//        Table table = tableEnv.sqlQuery(querySql);
//        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(table, Row.class);
//        rowDataStream.print();
//        env.execute("[Kafka Real Time]");
    }
}
