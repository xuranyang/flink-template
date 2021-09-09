package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlMysqlTableTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        /**
         * CREATE TABLE `mysql_test_table` (
         *   `name` varchar(255) DEFAULT NULL,
         *   `club` varchar(255) DEFAULT NULL
         * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
         */
        // executeSql or sqlUpdate
        tableEnv.executeSql("CREATE TABLE flink_test_table(" +
                "name STRING," +
                "club STRING" +
                ")" +
                "WITH" +
                "(" +
                "   'connector.type' = 'jdbc'," +
                "   'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink_test_db?serverTimezone=UTC'," +
                "   'connector.table' = 'mysql_test_table'," +
                "   'connector.driver' = 'com.mysql.cj.jdbc.Driver'," +
                "   'connector.username' = 'root'," +
                "   'connector.password' = '123456'," +
                "   'connector.lookup.cache.max-rows' = '5000'," +
                "   'connector.lookup.cache.ttl' = '10min'" +
                ")");

        String querySql = "SELECT name,club FROM flink_test_table";
        Table table = tableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("[MysqlInfo]");

    }
}


/**
 * CREATE TABLE MySQLTable (
 *   ...
 * ) WITH (
 *   'connector.type' = 'jdbc', -- 必选: jdbc方式
 *   'connector.url' = 'jdbc:mysql://localhost:3306/flink-test', -- 必选: JDBC url
 *   'connector.table' = 'jdbc_table_name',  -- 必选: 表名
 *    -- 可选: JDBC driver，如果不配置，会自动通过url提取
 *   'connector.driver' = 'com.mysql.jdbc.Driver',
 *   'connector.username' = 'name', -- 可选: 数据库用户名
 *   'connector.password' = 'password',-- 可选: 数据库密码
 *     -- 可选, 将输入进行分区的字段名.
 *   'connector.read.partition.column' = 'column_name',
 *     -- 可选, 分区数量.
 *   'connector.read.partition.num' = '50',
 *     -- 可选, 第一个分区的最小值.
 *   'connector.read.partition.lower-bound' = '500',
 *     -- 可选, 最后一个分区的最大值
 *   'connector.read.partition.upper-bound' = '1000',
 *     -- 可选, 一次提取数据的行数，默认为0，表示忽略此配置
 *   'connector.read.fetch-size' = '100',
 *    -- 可选, lookup缓存数据的最大行数，如果超过改配置，老的数据会被清除
 *   'connector.lookup.cache.max-rows' = '5000',
 *    -- 可选，lookup缓存存活的最大时间，超过该时间旧数据会过时，注意cache.max-rows与cache.ttl必须同时配置
 *   'connector.lookup.cache.ttl' = '10s',
 *    -- 可选, 查询数据最大重试次数
 *   'connector.lookup.max-retries' = '3',
 *    -- 可选,写数据最大的flush行数，默认5000，超过改配置，会触发刷数据
 *   'connector.write.flush.max-rows' = '5000',
 *    --可选，flush数据的间隔时间，超过该时间，会通过一个异步线程flush数据，默认是0s
 *   'connector.write.flush.interval' = '2s',
 *   -- 可选, 写数据失败最大重试次数
 *   'connector.write.max-retries' = '3'
 * )
 */