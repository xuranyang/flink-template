package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaCanalJson2StarRocksDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        env.setParallelism(1);
        env.enableCheckpointing(30000L);

        String sourceSql = "CREATE TABLE kafka_canal_json_source_table (\n" +
                "  id BIGINT,\n" +
                "  userid STRING\n," +
                "  age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'canal_test_topic',\n" +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'canal-json'\n" +
                ")";

        String starRocksSinkSql = "CREATE TABLE starrocks_sink_table(" +
                "id BIGINT," +
                "userid STRING," +
                "age INT," +
                "PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai'," +
                "'load-url'='127.0.0.1:8030'," +
                "'database-name' = 'tmp'," +
                "'table-name' = 'flink_cdc_pk_model_test'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'sink.buffer-flush.max-rows' = '1000000'," +
                "'sink.buffer-flush.max-bytes' = '300000000'," +
                "'sink.buffer-flush.interval-ms' = '5000'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'," +
                "'sink.max-retries' = '3'," +
                "'sink.properties.columns' = 'id,userid,age,__op'" +  //最后必须要指定一个虚拟字段__op
                ")";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(starRocksSinkSql);

//        String querySql = "SELECT * FROM kafka_canal_json_source_table";
//        tableEnv.executeSql(querySql).print();

        tableEnv.executeSql("insert into starrocks_sink_table select id,userid,age from kafka_canal_json_source_table");

        env.execute();
    }
}
