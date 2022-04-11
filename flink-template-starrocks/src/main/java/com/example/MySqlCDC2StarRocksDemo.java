package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MySQL建表语句:
 * CREATE TABLE flink_test.`flink_cdc_starrocks_test` (
 *   `id` bigint NOT NULL AUTO_INCREMENT,
 *   `userid` varchar(255) DEFAULT NULL,
 *   `age` int DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 *
 *
 * StarRocks建表语句:
 * CREATE TABLE IF NOT EXISTS tmp.flink_cdc_pk_model_test (
 * 	   id BIGINT NOT NULL,
 *     userid STRING,
 *     age INT
 * )
 * PRIMARY KEY(id)
 * DISTRIBUTED BY HASH(id) BUCKETS 8;
 */
//https://github.com/ververica/flink-cdc-connectors/blob/master/docs/content/connectors/mysql-cdc.md
public class MySqlCDC2StarRocksDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        SET 'execution.checkpointing.interval' = '10s';
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String mysqlSourceSql = "CREATE TABLE mysql_cdc_source_table (\n" +
                        " id BIGINT NOT NULL,\n" +
                        " userid STRING,\n" +
                        " age INT,\n" +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = 'localhost',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = '123456',\n" +
                        " 'database-name' = 'flink_test',\n" +
                        " 'table-name' = 'flink_cdc_starrocks_test'\n" +
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
        tableEnv.executeSql(mysqlSourceSql);
        tableEnv.executeSql(starRocksSinkSql);

//        tableEnv.executeSql("select * from mysql_cdc_source_table").print();
        tableEnv.executeSql("insert into starrocks_sink_table select id,userid,age from mysql_cdc_source_table");
//        tableEnv.executeSql("insert into starrocks_sink_table select * from mysql_cdc_source_table");

    }
}
