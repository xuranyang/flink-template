package com.sql.new_version.hive;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/hive/hive_read_write/
 *
 * *注:Hive表要修复元数据以后才能查询到数据
 * msck repair table tmp.hive_log;
 */
public class KafkaToHiveDemo {
    public static void main(String[] args) throws Exception {
        // 需要先生产 Kafka 测试数据
        KafkaToHiveDemoProduceMessage.produceKafkaMessage(10);
        TimeUnit.SECONDS.sleep(10);
        KafkaToHiveDemoProduceMessage.produceKafkaMessage(10);

        // 本地调试用
        System.setProperty("HADOOP_USER_NAME", "hive");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.12以后默认都是EventTime,这是过期方法,并且不在提IngestionTime
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //如果要使用ProcessingTime，可以关闭watermark
        env.getConfig().setAutoWatermarkInterval(0);

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        //设置为exactly-once
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));

        //配置hive
        String catalogName = "my_catalog";
        String db = "default";
        String hiveConfPath = "flink-template-core/src/main/resources";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, db, hiveConfPath);
        //注册并使用
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);

//        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS stream");
//        tEnv.executeSql("DROP TABLE IF EXISTS kafka_log");
        //创建kafka的表
        tEnv.executeSql("create table if not exists kafka_log(\n" +
                "user_id String,\n" +
                "order_amount Double,\n" +
                "log_ts Timestamp(3),\n" +
                "WATERMARK FOR log_ts AS log_ts -INTERVAL '5' SECOND\n" +
                " )WITH(\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'HIVE_TEST_1',\n" +
                " 'properties.bootstrap.servers' = 'kafka01:9092',\n" +
                " 'properties.group.id' = 'kafka2hive-test-group',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                " )");
        //开始在hive中创建表
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS tmp");
//        tEnv.executeSql("DROP TABLE IF EXISTS tmp.hive_log");
        tEnv.executeSql(" create table if not exists tmp.hive_log(\n" +
                " user_id String,\n" +
                " order_amount double\n" +
                " )partitioned by (\n" +
                " dt STRING,\n" +
                " hr STRING\n" +
                " )stored as PARQUET\n" +
                " tblproperties(\n" +
                " 'sink.partition-commit.trigger' = 'partition-time',\n" +
                " 'sink.partition-commit.delay' = '1min',\n" +
                " 'format' = 'json',\n" +
                " 'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
                " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'\n" +
                " )");
        //将kafka中的数据插入到hive中
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("insert into tmp.hive_log \n" +
                "  select \n" +
                "  user_id,\n" +
                "  order_amount,\n" +
                "  DATE_FORMAT(log_ts,'yyyy-MM-dd'),\n" +
                "  DATE_FORMAT(log_ts,'HH')\n" +
                "  from kafka_log");
    }

}
