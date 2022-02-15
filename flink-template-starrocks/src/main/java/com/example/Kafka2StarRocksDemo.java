package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2StarRocksDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        sqlStreamSink(tableEnv);

//        env.execute();
    }

    public static void sqlStreamSink(StreamTableEnvironment tableEnv) {
        /**
         * scan.startup.mode指定了读取kafka的位置，有几个选项：
         *  group-offsets：从特定消费者组的 ZK / Kafka 代理中的承诺偏移量开始。
         *  earliest-offset: 从最早的偏移量开始。
         *  latest-offset: 从最新的偏移量开始。
         *  timestamp：从用户提供的每个分区的时间戳开始。
         *      如果timestamp指定，则需要另一个配置选项scan.startup.timestamp-millis来指定时间戳
         *  specific-offsets：从用户提供的每个分区的特定偏移量开始。
         *      如果specific-offsets指定，则需要另一个配置选项scan.startup.specific-offsets来指定每个分区的特定启动偏移量，
         *      例如选项值partition:0,offset:42;partition:1,offset:300
         *
         * JsonFormat
         * {
         *   "timestamp": 1644907939254,
         *   "age": "18",
         *   "id": "1001"
         * }
         */
        String sourceSql = "CREATE TABLE source_table (\n" +
                " id STRING,\n" +
                " age INT,\n" +
                " `timestamp` BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'TEST_KAFKA_GROUP',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        /**
         * DDL:
         * CREATE TABLE IF NOT EXISTS test.kafka_info_test (
         *     id VARCHAR(255) NOT NULL COMMENT "id",
         * 	   age INT NOT NULL COMMENT "age",
         *     ts BIGINT NOT NULL COMMENT "ts"
         * )
         * DUPLICATE KEY(id,age,ts)
         * DISTRIBUTED BY HASH(id) BUCKETS 8;
         */
        String sinkSql = "CREATE TABLE sink_table(" +
                "id VARCHAR," +
                "age INT," +
                "ts BIGINT" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai'," +
                "'load-url'='127.0.0.1:8030'," +
                "'database-name' = 'test'," +
                "'table-name' = 'kafka_info_test'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'sink.buffer-flush.max-rows' = '1000000'," +
                "'sink.buffer-flush.max-bytes' = '300000000'," +
                "'sink.buffer-flush.interval-ms' = '5000'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'," +
                "'sink.max-retries' = '3'," +
                "'sink.properties.columns' = 'id,age,ts'" +
                ")";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);

//        tableEnv.executeSql("select id,age,`timestamp` from source_table").print();
        tableEnv.executeSql("insert into sink_table select id,age,`timestamp` from source_table");
    }
}
