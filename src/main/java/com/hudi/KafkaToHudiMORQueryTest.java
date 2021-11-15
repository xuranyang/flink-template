package com.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToHudiMORQueryTest {
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


        String querySql = "select * from hudi_table_sink";
        tableEnv.executeSql(hudiSinkSql);
        tableEnv.executeSql(querySql).print();

    }
}
