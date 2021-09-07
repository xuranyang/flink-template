package com.sql.new_version;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CatalogTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        String catalog_name = "myhive_catalog";
        String defaultDatabase = "default";

        // hive-site.xml路径
        String hiveConfDir = "/etc/hive/conf";

//        String version = "2.1.1";

        HiveCatalog hiveCatalog = new HiveCatalog(catalog_name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive_catalog", hiveCatalog);
        tableEnv.useCatalog("myhive_catalog");

//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");

        String[] databases = tableEnv.listDatabases();
        for (String database : databases) {
            System.out.println(database);
        }

        String[] tables = tableEnv.listTables();
        for (String table : tables) {
            System.out.println(table);
        }

//        String sql = "select * from xxx_table limit 10";
//        Table table = tableEnv.sqlQuery(sql);

//        tableEnv.toAppendStream(table, Row.class).print();

//        tableEnv.executeSql(sql).print();

//        env.execute();
    }
}
