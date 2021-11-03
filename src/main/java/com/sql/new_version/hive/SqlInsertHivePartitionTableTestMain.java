package com.sql.new_version.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 用FlinkSQL写入Hive的分区表（非分区表没有该问题）时会出现元数据metastore缺失问题,导致数据已经成功写入Hive表对应的HDFS路径,但是却无法查询到数据
 * 需要 add partition 或者 msck repair table修复元数据之后,才能查询到
 * 这里使用的是流模式,每隔一个checkpoint时间就会往hive表中commit一次数据
 * alter table tmp.flink_test_table_partition add partition(dt='2021-01-01');
 * or
 * msck repair table tmp.flink_test_table_partition;
 * select * from tmp.flink_test_table_partition;
 */
public class SqlInsertHivePartitionTableTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String catalog_name = "myhive";
        String defaultDatabase = "default";

        // hive-site.xml路径
//        String hiveConfDir = "/etc/hive/conf";
        String hiveConfDir = "src/main/resources";

//        String version = "2.1.1";

        HiveCatalog hiveCatalog = new HiveCatalog(catalog_name, defaultDatabase, hiveConfDir);
//        HiveCatalog hiveCatalog = new HiveCatalog(catalog_name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("tmp");

        /*
        DROP TABLE IF EXISTS tmp.flink_test_table_partition;
        CREATE TABLE IF NOT EXISTS tmp.flink_test_table_partition
        (
            `name`  string,
            `age`   int
        ) COMMENT 'test partition table'
        PARTITIONED BY (`dt` string)
        stored as orc
        tblproperties (
        'orc.compress'='snappy',
        'sink.partition-commit.policy.kind'='metastore'
        );
         */
        String sql = "insert into tmp.flink_test_table_partition partition(dt='2021-01-01') select 'maybe',26";
        // table query
        tableEnv.executeSql(sql).print();

    }
}
