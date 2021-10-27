package com.cdc.sql_cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * sql-cdc 只适合单库或者单表,无法获取多表的binlog
 * <p>
 * use flink_cdc_test;
 * <p>
 * CREATE TABLE `cdc_source_table` (
 * `id` bigint NOT NULL AUTO_INCREMENT,
 * `name` varchar(255) DEFAULT NULL,
 * `age` int DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 * <p>
 * CREATE TABLE cdc_target_table LIKE cdc_source_table;
 */
public class FlinkMySqlCdcDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String sourceSql = "CREATE TABLE source_table (\n" +
                " id BIGINT NOT NULL,\n" +
                " name STRING,\n" +
                " age INT,\n" +
                " PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
//                " 'scan.startup.mode' = 'initial',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'flink_cdc_test',\n" +
                " 'table-name' = 'cdc_source_table'\n" +
                ")";

        String sinkSql = "CREATE TABLE sink_table (\n" +
                " id BIGINT NOT NULL,\n" +
                " name STRING,\n" +
                " age INT,\n" +
                " PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                " 'url' = 'jdbc:mysql://localhost:3306/flink_cdc_test?serverTimezone=Asia/Shanghai',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'table-name' = 'cdc_target_table'\n" +
                ")";

        // 注意SQL结尾不能加;号
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);

        /**
         * 方法一 直接insert
         */
        String insertSql = "insert into sink_table select id,UPPER(name),age from source_table";
        tableEnv.executeSql(insertSql);

        /**
         * 方法二 使用executeInsert方法
         */
//        String insertSql = "SELECT id, UPPER(name), age FROM source_table";
//        Table table = tableEnv.sqlQuery(insertSql);
//        table.executeInsert("sink_table");

//        tableEnv.toRetractStream(table, Row.class).print();
//        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);
//        dataStream.print();
//        env.execute();
    }
}
