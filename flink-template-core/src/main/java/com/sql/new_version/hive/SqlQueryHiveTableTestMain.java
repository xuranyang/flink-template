package com.sql.new_version.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

/**
 * 如果HDFS启用了HA高可用
 * 在本地的hosts文件中需要配置一下 nameservice 的地址映射
 * 将hive-site.xml拷贝到项目指定路径下
 */
public class SqlQueryHiveTableTestMain {
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
        String hiveConfDir = "flink-template-core/src/main/resources";

//        String version = "2.1.1";

        HiveCatalog hiveCatalog = new HiveCatalog(catalog_name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("ods");

        String[] databases = tableEnv.listDatabases();
        for (String database : databases) {
            System.out.println(database);
        }

        String[] tables = tableEnv.listTables();
        for (String table : tables) {
            System.out.println(table);
        }

        String sql = "select * from ods_test_table where dt='2020-01-01' limit 10";
        // table查询
        tableEnv.executeSql(sql).print();

        // DataStream流查询
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
