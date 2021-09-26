package com.sql.new_version;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/hbase/
 */
public class SqlHBaseTableBatchTestDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(newStreamSettings);

        /**
         * create 'flink_sql_test',{NAME=>'cf1'}
         * put 'flink_sql_test','rk01','cf1:name','zhangsan'
         * put 'flink_sql_test','rk02','cf1:name','lisi'
         * put 'flink_sql_test','rk03','cf1:name','zhangsan'
         * +--------------------------------+----------------------+
         * |                           name |                  num |
         * +--------------------------------+----------------------+
         * |                           lisi |                    1 |
         * |                       zhangsan |                    2 |
         * +--------------------------------+----------------------+
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

        String querySql = "SELECT name,count(1) as num FROM test_table group by name";

        tableEnv.executeSql(querySql).print();

        CloseableIterator<Row> collect = tableEnv.executeSql(querySql).collect();

        while (collect.hasNext()) {
            Row next = collect.next();
            String rowKind = next.getKind().toString();
            System.out.println(rowKind);

            int arity = next.getArity();
            for (int i = 0; i < arity; i++) {
                System.out.println(next.getField(i));
            }

        }

    }
}
