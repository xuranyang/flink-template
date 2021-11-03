package com.sql.new_version.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * if use HDFS HA
 * hosts file add nameservice mapping
 */
public class SqlInsertHiveTableTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

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
        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("tmp");

        /*
        DROP TABLE IF EXISTS tmp.flink_test_table;
        CREATE TABLE IF NOT EXISTS tmp.flink_test_table
        (
            `name`  string,
            `age`   int
        ) COMMENT 'test table'
        stored as orc
        tblproperties (
        'orc.compress'='snappy',
        'sink.partition-commit.policy.kind'='metastore'
        );
         */
        String sql = "insert into tmp.flink_test_table select 'maybe',26";
        // table query
        tableEnv.executeSql(sql);

    }
}
