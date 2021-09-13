package com.sql.new_version;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/hbase/
 */
public class SqlHBaseTableTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        /**
         * create 'flink_sql_test',{NAME=>'cf1'}
         * put 'flink_sql_test','rk01','cf1:name','zhangsan'
         * put 'flink_sql_test','rk01','cf1:age','18'
         * put 'flink_sql_test','rk02','cf1:name','lisi'
         * put 'flink_sql_test','rk02','cf1:age','20'
         * scan 'flink_sql_test'
         */
        // executeSql or sqlUpdate
        tableEnv.executeSql("CREATE TABLE test_table(" +
                " rowkey STRING,\n" +
                " cf1 ROW<name STRING,age STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                " ) WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'flink_sql_test',\n" +
                " 'zookeeper.quorum' = 'zk01:2181,zk02:2181,zk03:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase'\n" +
                " )");

//        String querySql = "SELECT * FROM test_table";
        String querySql = "SELECT rowkey,name,age FROM test_table";
        Table table = tableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("[HBaseInfo]");

    }
}
