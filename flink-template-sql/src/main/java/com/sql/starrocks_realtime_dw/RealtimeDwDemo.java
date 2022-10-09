package com.sql.starrocks_realtime_dw;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/*
CREATE TABLE IF NOT EXISTS tmp.dws_user_recharge_login
(
userId STRING,
window_end DATETIME,
login_num INT,
first_login_time DATETIME,
last_login_time DATETIME,
recharge_num INT,
total_recharge_amount DOUBLE,
first_recharge_time DATETIME,
last_recharge_time DATETIME
)
UNIQUE KEY(userId,window_end)
PARTITION BY RANGE(window_end)(
    START ("2022-10-01") END ("2022-11-01") EVERY (INTERVAL 1 day)
)
DISTRIBUTED BY HASH(userId,window_end) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-31",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);


CREATE TABLE IF NOT EXISTS tmp.dws_user_recharge_login_join_dim
(
userId STRING,
window_end DATETIME,
gender STRING,
region INT,
login_num INT,
first_login_time DATETIME,
last_login_time DATETIME,
recharge_num INT,
total_recharge_amount DOUBLE,
first_recharge_time DATETIME,
last_recharge_time DATETIME
)
UNIQUE KEY(userId,window_end)
PARTITION BY RANGE(window_end)(
    START ("2022-10-01") END ("2022-11-01") EVERY (INTERVAL 1 day)
)
DISTRIBUTED BY HASH(userId,window_end) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-31",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);
 */
public class RealtimeDwDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 声明为流任务
                //.inBatchMode() // 声明为批任务
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        TableConfig config = tableEnv.getConfig();
        config.setLocalTimeZone(ZoneId.of("GMT+02:00"));

        tableEnv.executeSql("create table dwd_kafka_user_login\n" +
                "(\n" +
                "    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "    `partition`  BIGINT METADATA VIRTUAL,\n" +
                "    `offset`     BIGINT METADATA VIRTUAL,\n" +
                "    `userId`     String,\n" +
                "    `loginTs`    BIGINT,\n" +
                "    ts AS TO_TIMESTAMP(FROM_UNIXTIME(loginTs / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    login_ts_ltz AS TO_TIMESTAMP_LTZ(loginTs, 3),\n" +
                "    WATERMARK FOR login_ts_ltz AS login_ts_ltz - INTERVAL '1' SECOND\n" +
                ") with (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'CANAL_TEST_1',\n" +
                "      'properties.bootstrap.servers' = 'test-kafka:9092',\n" +
                "      'format' = 'json',\n" +
                "      'properties.group.id' = 'testGroup',\n" +
                "      'scan.startup.mode' = 'earliest-offset',\n" +
                "      'json.ignore-parse-errors' = 'true'\n" +
                "      )");

        tableEnv.executeSql("create table dwd_kafka_user_recharge\n" +
                "(\n" +
                "    `event_time`     TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "    `partition`      BIGINT METADATA VIRTUAL,\n" +
                "    `offset`         BIGINT METADATA VIRTUAL,\n" +
                "    `userId`         String,\n" +
                "    `rechargeType`   INT,\n" +
                "    `rechargeAmount` DOUBLE,\n" +
                "    `rechargeTs`     BIGINT,\n" +
                "    recharge_ts_ltz AS TO_TIMESTAMP_LTZ(rechargeTs, 3),\n" +
                "    WATERMARK FOR recharge_ts_ltz AS recharge_ts_ltz - INTERVAL '1' SECOND\n" +
                ") with (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'CANAL_TEST_2',\n" +
                "      'properties.bootstrap.servers' = 'test-kafka:9092',\n" +
                "      'format' = 'json',\n" +
                "      'properties.group.id' = 'testGroup',\n" +
                "      'scan.startup.mode' = 'earliest-offset',\n" +
                "      'json.ignore-parse-errors' = 'true'\n" +
                "      )");

        tableEnv.executeSql("CREATE VIEW t1 as \n" +
                "SELECT userId,\n" +
                "       count(1)          as login_num,\n" +
                "       min(login_ts_ltz) as first_login_time,\n" +
                "       max(login_ts_ltz) as last_login_time\n" +
                "FROM dwd_kafka_user_login\n" +
                "GROUP BY userId");

        tableEnv.executeSql("CREATE VIEW t2 as\n" +
                "SELECT userId,\n" +
                "       count(1)             as recharge_num,\n" +
                "       sum(rechargeAmount)  as total_recharge_amount,\n" +
                "       min(recharge_ts_ltz) as first_recharge_time,\n" +
                "       max(recharge_ts_ltz) as last_recharge_time\n" +
                "FROM dwd_kafka_user_recharge\n" +
                "GROUP BY userId");

        String joinSql = "SELECT if(t1.userId is null, t2.userId, t1.userId) as userId,\n" +
                "       login_num,\n" +
                "       first_login_time,\n" +
                "       last_login_time,\n" +
                "       recharge_num,\n" +
                "       total_recharge_amount,\n" +
                "       first_recharge_time,\n" +
                "       last_recharge_time\n" +
                "FROM t1 FULL JOIN t2 on t1.userId = t2.userId";

//        String userLoginSql = "select * from dwd_kafka_user_login";
//        String userRechargeSql = "select * from dwd_kafka_user_recharge";
//        tableEnv.executeSql(userLoginSql).print();
//        tableEnv.executeSql(userRechargeSql).print();

//        tableEnv.executeSql(joinSql).print();

        tableEnv.executeSql("SELECT userId,\n" +
                "--        TUMBLE_START(login_ts_ltz, INTERVAL '5' SECONDS)   AS window_start,\n" +
                "       UNIX_TIMESTAMP(CAST(tumble_start(login_ts_ltz, interval '5' SECONDS) AS STRING)) * 1000  as window_start,\n" +
                "       TUMBLE_END(login_ts_ltz, INTERVAL '5' SECONDS)     AS window_end,\n" +
                "       TUMBLE_ROWTIME(login_ts_ltz, INTERVAL '5' SECONDS) as window_rowtime,\n" +
                "       count(1)                                           as login_num,\n" +
                "       min(login_ts_ltz)                                  as first_login_time,\n" +
                "       max(login_ts_ltz)                                  as last_login_time\n" +
                "FROM dwd_kafka_user_login\n" +
                "GROUP BY userId, TUMBLE(login_ts_ltz, INTERVAL '5' SECONDS)");

        tableEnv.executeSql("SELECT userId,\n" +
                "--        TUMBLE_START(recharge_ts_ltz, INTERVAL '5' SECONDS)   AS window_start,\n" +
                "       UNIX_TIMESTAMP(CAST(tumble_start(recharge_ts_ltz, interval '5' SECONDS) AS STRING)) * 1000  as window_start,\n" +
                "       TUMBLE_END(recharge_ts_ltz, INTERVAL '5' SECONDS)     AS window_end,\n" +
                "       TUMBLE_ROWTIME(recharge_ts_ltz, INTERVAL '5' SECONDS) as window_rowtime,\n" +
                "       count(1)                                              as recharge_num,\n" +
                "       sum(rechargeAmount)                                   as total_recharge_amount,\n" +
                "       min(recharge_ts_ltz)                                  as first_recharge_time,\n" +
                "       max(recharge_ts_ltz)                                  as last_recharge_time\n" +
                "FROM dwd_kafka_user_recharge\n" +
                "GROUP BY userId, TUMBLE(recharge_ts_ltz, INTERVAL '5' SECONDS)");

        tableEnv.executeSql("CREATE VIEW view_dws_user_recharge_login AS\n" +
                "SELECT userId,\n" +
                "       TUMBLE_START(ts_ltz, INTERVAL '5' SECONDS)                                             AS window_start,\n" +
                "       TUMBLE_END(ts_ltz, INTERVAL '5' SECONDS)                                               AS window_end,\n" +
                "       TUMBLE_ROWTIME(ts_ltz, INTERVAL '5' SECONDS)                                           as window_rowtime,\n" +
                "       COUNT(case when table_type = 'login' then 1 else null end)                             as login_num,\n" +
                "       MIN(case when table_type = 'login' then ts_ltz else TO_TIMESTAMP('9999-12-31 00:00:00') end)    as first_login_time,\n" +
                "       MAX(case when table_type = 'login' then ts_ltz else TO_TIMESTAMP('1000-01-01 00:00:00') end)    as last_login_time,\n" +
                "       COUNT(case when table_type = 'recharge' then 1 else null end)                          as recharge_num,\n" +
                "       SUM(rechargeAmount)                                                                    as total_recharge_amount,\n" +
                "       MIN(case when table_type = 'recharge' then ts_ltz else TO_TIMESTAMP('9999-12-31 00:00:00') end) as first_recharge_time,\n" +
                "       MAX(case when table_type = 'recharge' then ts_ltz else TO_TIMESTAMP('1000-01-01 00:00:00') end) as last_recharge_time\n" +
                "FROM (\n" +
                "   SELECT userId,\n" +
                "          login_ts_ltz as ts_ltz,\n" +
                "          -1         as rechargeType,\n" +
                "          0         as rechargeAmount,\n" +
                "          'login'      as table_type\n" +
                "   FROM dwd_kafka_user_login\n" +
                "   UNION ALL\n" +
                "   SELECT userId, recharge_ts_ltz as ts_ltz, rechargeType, rechargeAmount, 'recharge' as table_type\n" +
                "   FROM dwd_kafka_user_recharge\n" +
                ") t0\n" +
                "GROUP BY userId, TUMBLE(ts_ltz, INTERVAL '5' SECONDS)");

//        tableEnv.executeSql("select * from view_dws_user_recharge_login").print();

        tableEnv.executeSql("CREATE TABLE sink_sr_dws_user_recharge_login(\n" +
                "    userId STRING,\n" +
                "    window_end TIMESTAMP,\n" +
                "    login_num BIGINT,\n" +
                "    first_login_time TIMESTAMP,\n" +
                "    last_login_time TIMESTAMP,\n" +
                "    recharge_num BIGINT,\n" +
                "    total_recharge_amount DOUBLE,\n" +
                "    first_recharge_time TIMESTAMP,\n" +
                "    last_recharge_time TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector' = 'starrocks',\n" +
                "    'jdbc-url'='jdbc:mysql://test-starrocks:9030?serverTimezone=Asia/Shanghai',\n" +
                "    'load-url'='test-starrocks:8035',\n" +
                "    'database-name' = 'tmp',\n" +
                "    'table-name' = 'dws_user_recharge_login',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'sink.buffer-flush.max-rows' = '1000000',\n" +
                "    'sink.buffer-flush.max-bytes' = '300000000',\n" +
                "    'sink.buffer-flush.interval-ms' = '5000',\n" +
                "    'sink.properties.column_separator' = '\\x01',\n" +
                "    'sink.properties.row_delimiter' = '\\x02',\n" +
                "    'sink.max-retries' = '3',\n" +
                "    'sink.properties.columns' = 'userId, window_end,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time'\n" +
                ")");

//        tableEnv.executeSql("INSERT INTO sink_sr_dws_user_recharge_login\n" +
//                "SELECT userId, window_end,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time FROM view_dws_user_recharge_login");

        tableEnv.executeSql("CREATE TABLE dim_user(\n" +
                "    rowkey STRING,\n" +
                "    base_info ROW<gender STRING,region STRING>,\n" +
                "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'dim_user_test',\n" +
                "    'zookeeper.quorum' = 'fat01:2181,fat02:2181,fat03:2181',\n" +
                "    'zookeeper.znode.parent' = '/hbase'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink_sr_dws_user_recharge_login_join_dim(\n" +
                "    userId STRING,\n" +
                "    window_end TIMESTAMP,\n" +
                "    gender STRING,\n" +
                "    region INT,\n" +
                "    login_num BIGINT,\n" +
                "    first_login_time TIMESTAMP,\n" +
                "    last_login_time TIMESTAMP,\n" +
                "    recharge_num BIGINT,\n" +
                "    total_recharge_amount DOUBLE,\n" +
                "    first_recharge_time TIMESTAMP,\n" +
                "    last_recharge_time TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector' = 'starrocks',\n" +
                "    'jdbc-url'='jdbc:mysql://test-starrocks:9030?serverTimezone=Asia/Shanghai',\n" +
                "    'load-url'='test-starrocks:8035',\n" +
                "    'database-name' = 'tmp',\n" +
                "    'table-name' = 'dws_user_recharge_login_join_dim',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'sink.buffer-flush.max-rows' = '1000000',\n" +
                "    'sink.buffer-flush.max-bytes' = '300000000',\n" +
                "    'sink.buffer-flush.interval-ms' = '5000',\n" +
                "    'sink.properties.column_separator' = '\\x01',\n" +
                "    'sink.properties.row_delimiter' = '\\x02',\n" +
                "    'sink.max-retries' = '3',\n" +
                "    'sink.properties.columns' = 'userId,window_end,gender,region,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time'\n" +
                "    )");

//        tableEnv.executeSql("INSERT INTO sink_sr_dws_user_recharge_login_join_dim\n" +
//                "SELECT t1.userId,window_end,t2.gender,cast(t2.region as int) as region,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time\n" +
//                "FROM view_dws_user_recharge_login t1 LEFT JOIN dim_user t2 on t1.userId=t2.userId");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink_sr_dws_user_recharge_login\n" +
                "SELECT userId, window_end,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time FROM view_dws_user_recharge_login");
        statementSet.addInsertSql("INSERT INTO sink_sr_dws_user_recharge_login_join_dim\n" +
                "SELECT t1.userId,window_end,t2.gender,cast(t2.region as int) as region,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time\n" +
                "FROM view_dws_user_recharge_login t1 LEFT JOIN dim_user t2 on t1.userId=t2.rowkey");
        statementSet.execute();
    }
}
