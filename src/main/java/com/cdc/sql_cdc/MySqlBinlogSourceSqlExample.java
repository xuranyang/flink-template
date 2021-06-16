package com.cdc.sql_cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * sql-cdc 只适合单库或者单表,无法获取多表的binlog
 */
public class MySqlBinlogSourceSqlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " weight DECIMAL(10,3)\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'flinkuser',\n" +
                " 'password' = 'flinkpw',\n" +
                " 'database-name' = 'inventory',\n" +
                " 'table-name' = 'products'\n" +
                ");");

        Table table = tableEnv.sqlQuery("SELECT id, UPPER(name), description, weight FROM mysql_binlog");

//        tableEnv.toAppendStream(table, Row.class).print();
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);
        dataStream.print();

        env.execute();
    }
}
