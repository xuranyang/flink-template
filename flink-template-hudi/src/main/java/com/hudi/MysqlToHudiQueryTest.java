package com.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 0.9.0的代码没有兼容windows
 * 目前windows系统写入Hudi,路径会存在反斜杠问题
 * Not in marker dir. Marker Path
 */
public class MysqlToHudiQueryTest {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        env.enableCheckpointing(5000);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String sinkSql = "CREATE TABLE hudi_mysql_table_sink(\n" +
                "  id BIGINT NOT NULL,\n" +
                "  name VARCHAR(20),\n" +
                "  age INT\n" +
//                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ")\n" +
//                "PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://test01:8020/test/hudi/hudi_mysql_table_sink',\n" +
                "  'read.streaming.enabled' = 'true',\n" +
                "  'hoodie.datasource.write.recordkey.field' = 'id',\n" +
                "  'write.precombine.field'='id',\n" +
                "  'compaction.schedule.enabled'='false',\n" +
                "  'compaction.async.enabled'='false',\n" +
                "  'write.tasks' = '1',\n" + //需要添加这个参数否则容易Timeout
                "  'write.batch.size' = '1',\n" +
                "  'table.type' = 'MERGE_ON_READ'\n" +
//                "  'table.type' = 'COPY_ON_WRITE'\n" +
                ")";

        String querySql = "select * from hudi_mysql_table_sink";
        tableEnv.executeSql(sinkSql);
//        tableEnv.executeSql(insertSql).print();
        tableEnv.executeSql(querySql).print();

    }
}
