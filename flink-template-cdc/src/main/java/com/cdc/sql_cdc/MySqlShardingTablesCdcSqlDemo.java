package com.cdc.sql_cdc;

/*
CREATE DATABASE db_1;
USE db_1;
 CREATE TABLE user_1 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
 INSERT INTO user_1 VALUES (110,"user_110","Shanghai","123567891234","user_110@foo.com");

 CREATE TABLE user_2 (
   id INTEGER NOT NULL PRIMARY KEY,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512),
   email VARCHAR(255)
 );
INSERT INTO user_2 VALUES (120,"user_120","Shanghai","123567891234","user_120@foo.com");

CREATE DATABASE db_2;
USE db_2;
CREATE TABLE user_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  email VARCHAR(255)
);
INSERT INTO user_1 VALUES (110,"user_110","Shanghai","123567891234", NULL);

CREATE TABLE user_2 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  email VARCHAR(255)
);
INSERT INTO user_2 VALUES (220,"user_220","Shanghai","123567891234","user_220@foo.com");


CREATE TABLE bigdata.user_0 (
  id INTEGER NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  email VARCHAR(255),
  database_name VARCHAR(255),
  table_name VARCHAR(255),
  PRIMARY KEY (id,database_name,table_name)
);

-- db_1
INSERT INTO db_1.user_1 VALUES (111,"user_111","Shanghai","123567891234","user_111@foo.com");
-- db_1
UPDATE db_1.user_2 SET address='Beijing' WHERE id=120;
-- db_2
DELETE FROM db_2.user_2 WHERE id=220;
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/build-real-time-data-lake-tutorial-zh.html
 * https://ververica.github.io/flink-cdc-connectors/release-2.3/content/connectors/mysql-cdc.html
 */
public class MySqlShardingTablesCdcSqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String mysqlCdcSourceSql = "CREATE TABLE user_source (\n" +
                "    database_name STRING METADATA VIRTUAL,\n" +
                "    table_name STRING METADATA VIRTUAL,\n" +
                "    `id` DECIMAL(20, 0) NOT NULL,\n" +
                "    name STRING,\n" +
                "    address STRING,\n" +
                "    phone_number STRING,\n" +
                "    email STRING,\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                // initial/earliest-offset/latest-offset/specific-offset/timestamp
                "    'scan.startup.mode' = 'initial',\n" +
                "    'hostname' = '127.0.0.1',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root00',\n" +
                "    'database-name' = 'db_[0-9]+',\n" +
                "    'table-name' = 'user_[0-9]+'\n" +
                "  )";

        tableEnv.executeSql(mysqlCdcSourceSql);
//        tableEnv.executeSql("select * from user_source").print();

        String sinkJdbcSql = "CREATE TABLE sink_table (\n" +
                "    `id` DECIMAL(20, 0) NOT NULL,\n" +
                "    name STRING,\n" +
                "    address STRING,\n" +
                "    phone_number STRING,\n" +
                "    email STRING,\n" +
                "    database_name STRING,\n" +
                "    table_name STRING,\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                " 'url' = 'jdbc:mysql://localhost:3306/bigdata?serverTimezone=Asia/Shanghai',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'root00',\n" +
                " 'table-name' = 'user_0'\n" +
                ")";
        tableEnv.executeSql(sinkJdbcSql);
        tableEnv.executeSql("insert into sink_table select id,name,address,phone_number,email,database_name,table_name from user_source").print();
        env.execute();
    }
}
