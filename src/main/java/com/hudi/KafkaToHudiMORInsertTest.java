package com.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToHudiMORInsertTest {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        env.enableCheckpointing(5000);

//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String kafkaSourceSql = "create table kafka_table_source(\n" +
                "name string,\n" +
                "amount int\n" +
                ") with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'canal_test_topic',\n" +
                "'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'properties.group.id' = 'testGroup'\n" +
                ")";

        String hudiSinkSql = "CREATE TABLE hudi_table_sink(\n" +
                "  name VARCHAR(20),\n" +
                "  amount INT\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://test01:8020/test/hudi/hudi_table_sink',\n" +
                "  'read.streaming.enabled' = 'true',\n" +
                "  'hoodie.datasource.write.recordkey.field' = 'name',\n" +
                "  'write.precombine.field'='name',\n" +
                "  'metadata.compaction.delta_commits' = '1',\n" +
//                "  'compaction.schedule.enabled'='false',\n" +
//                "  'compaction.async.enabled'='false',\n" +
                "  'table.type' = 'MERGE_ON_READ'\n" +
//                "  'table.type' = 'COPY_ON_WRITE'\n" +
                ")";

        // hadoop fs -rm -r /test/hudi/hudi_table_sink/
        String insertSql = "insert into hudi_table_sink select name,amount from kafka_table_source";
        tableEnv.executeSql(kafkaSourceSql);
        tableEnv.executeSql(hudiSinkSql);
        tableEnv.executeSql(insertSql).print();

//        String querySql = "select * from hudi_table_sink";
//        tableEnv.executeSql(querySql).print();
    }
}
