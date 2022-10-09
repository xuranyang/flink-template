-- dwd_kafka_user_login
create table dwd_kafka_user_login
(
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    `partition`  BIGINT METADATA VIRTUAL,
    `offset`     BIGINT METADATA VIRTUAL,
    `userId`     String,
    `loginTs`    BIGINT,
    ts AS TO_TIMESTAMP(FROM_UNIXTIME(loginTs / 1000, 'yyyy-MM-dd HH:mm:ss')),
    login_ts_ltz AS TO_TIMESTAMP_LTZ(loginTs, 3),
    WATERMARK FOR login_ts_ltz AS login_ts_ltz - INTERVAL '1' SECOND
) with (
      'connector' = 'kafka',
      'topic' = 'CANAL_TEST_1',
      'properties.bootstrap.servers' = 'test-kafka:9092',
      'format' = 'json',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'json.ignore-parse-errors' = 'true'
      )
;

-- dwd_kafka_user_recharge
create table dwd_kafka_user_recharge
(
    `event_time`     TIMESTAMP(3) METADATA FROM 'timestamp',
    `partition`      BIGINT METADATA VIRTUAL,
    `offset`         BIGINT METADATA VIRTUAL,
    `userId`         String,
    `rechargeType`   INT,
    `rechargeAmount` DOUBLE,
    `rechargeTs`     BIGINT,
    recharge_ts_ltz AS TO_TIMESTAMP_LTZ(rechargeTs, 3),
    WATERMARK FOR recharge_ts_ltz AS recharge_ts_ltz - INTERVAL '1' SECOND
) with (
      'connector' = 'kafka',
      'topic' = 'CANAL_TEST_2',
      'properties.bootstrap.servers' = 'test-kafka:9092',
      'format' = 'json',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'json.ignore-parse-errors' = 'true'
      )
;

CREATE VIEW t1 as
SELECT userId,
       count(1)          as login_num,
       min(login_ts_ltz) as first_login_time,
       max(login_ts_ltz) as last_login_time
FROM dwd_kafka_user_login
GROUP BY userId
;

CREATE VIEW t2 as
SELECT userId,
       count(1)             as recharge_num,
       sum(rechargeAmount)  as total_recharge_amount,
       min(recharge_ts_ltz) as first_recharge_time,
       max(recharge_ts_ltz) as last_recharge_time
FROM dwd_kafka_user_recharge
GROUP BY userId
;


SELECT if(t1.userId is null, t2.userId, t1.userId) as userId,
       login_num,
       first_login_time,
       last_login_time,
       recharge_num,
       total_recharge_amount,
       first_recharge_time,
       last_recharge_time
FROM t1 FULL JOIN t2 on t1.userId = t2.userId
;

-- 窗口函数聚合
-- CREATE VIEW window_t1 as
SELECT userId,
       TUMBLE_START(login_ts_ltz, INTERVAL '5' SECONDS)   AS window_start,
--        UNIX_TIMESTAMP(CAST(tumble_start(login_ts_ltz, interval '5' SECONDS) AS STRING)) * 1000  as window_start,
       TUMBLE_END(login_ts_ltz, INTERVAL '5' SECONDS)     AS window_end,
       TUMBLE_ROWTIME(login_ts_ltz, INTERVAL '5' SECONDS) as window_rowtime,
       count(1)                                           as login_num,
       min(login_ts_ltz)                                  as first_login_time,
       max(login_ts_ltz)                                  as last_login_time
FROM dwd_kafka_user_login
GROUP BY userId, TUMBLE(login_ts_ltz, INTERVAL '5' SECONDS)
;


CREATE VIEW view_dws_user_recharge_login AS
SELECT userId,
       TUMBLE_START(ts_ltz, INTERVAL '5' SECONDS)                                             AS window_start,
       TUMBLE_END(ts_ltz, INTERVAL '5' SECONDS)                                               AS window_end,
       TUMBLE_ROWTIME(ts_ltz, INTERVAL '5' SECONDS)                                           as window_rowtime,
       COUNT(case when table_type = 'login' then 1 else null end)                             as login_num,
       MIN(case when table_type = 'login' then ts_ltz else TO_TIMESTAMP('9999-12-31 00:00:00') end)    as first_login_time,
       MAX(case when table_type = 'login' then ts_ltz else TO_TIMESTAMP('1000-01-01 00:00:00') end)    as last_login_time,
       COUNT(case when table_type = 'recharge' then 1 else null end)                          as recharge_num,
       SUM(rechargeAmount)                                                                    as total_recharge_amount,
       MIN(case when table_type = 'recharge' then ts_ltz else TO_TIMESTAMP('9999-12-31 00:00:00') end) as first_recharge_time,
       MAX(case when table_type = 'recharge' then ts_ltz else TO_TIMESTAMP('1000-01-01 00:00:00') end) as last_recharge_time
FROM (
   SELECT userId,
          login_ts_ltz as ts_ltz,
          -1         as rechargeType,
          0         as rechargeAmount,
          'login'      as table_type
   FROM dwd_kafka_user_login
   UNION ALL
   SELECT userId, recharge_ts_ltz as ts_ltz, rechargeType, rechargeAmount, 'recharge' as table_type
   FROM dwd_kafka_user_recharge
) t0
GROUP BY userId, TUMBLE(ts_ltz, INTERVAL '5' SECONDS)
;



CREATE TABLE sink_sr_dws_user_recharge_login(
    userId STRING,
    window_end TIMESTAMP,
    login_num BIGINT,
    first_login_time TIMESTAMP,
    last_login_time TIMESTAMP,
    recharge_num BIGINT,
    total_recharge_amount DOUBLE,
    first_recharge_time TIMESTAMP,
    last_recharge_time TIMESTAMP
) WITH (
    'connector' = 'starrocks',
    'jdbc-url'='jdbc:mysql://test-starrocks:9030?serverTimezone=Asia/Shanghai',
    'load-url'='test-starrocks:8035',
    'database-name' = 'tmp',
    'table-name' = 'dws_user_recharge_login',
    'username' = 'root',
    'password' = '',
    'sink.buffer-flush.max-rows' = '1000000',
    'sink.buffer-flush.max-bytes' = '300000000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.column_separator' = '\x01',
    'sink.properties.row_delimiter' = '\x02',
    'sink.max-retries' = '3',
    'sink.properties.columns' = 'userId, window_end,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time'
)
;

INSERT INTO sink_sr_dws_user_recharge_login
SELECT userId, window_end,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time FROM view_dws_user_recharge_login
;

-- hbase
create 'dim_user_test',{NAME=>'base_info',COMPRESSION=>'SNAPPY'}
put 'dim_user_test','1001','base_info:gender','male'
put 'dim_user_test','1001','base_info:region','0'
put 'dim_user_test','1002','base_info:gender','male'
put 'dim_user_test','1002','base_info:region','0'
put 'dim_user_test','1003','base_info:gender','female'
put 'dim_user_test','1003','base_info:region','1'

CREATE TABLE dim_user(
    rowkey STRING,
    base_info ROW<gender STRING,region INT>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim_user_test',
    'zookeeper.quorum' = 'fat01:2181,fat02:2181,fat03:2181',
    'zookeeper.znode.parent' = '/hbase'
);

CREATE TABLE sink_sr_dws_user_recharge_login_join_dim(
    userId STRING,
    window_end TIMESTAMP,
    gender STRING,
    region INT,
    login_num BIGINT,
    first_login_time TIMESTAMP,
    last_login_time TIMESTAMP,
    recharge_num BIGINT,
    total_recharge_amount DOUBLE,
    first_recharge_time TIMESTAMP,
    last_recharge_time TIMESTAMP
) WITH (
    'connector' = 'starrocks',
    'jdbc-url'='jdbc:mysql://test-starrocks:9030?serverTimezone=Asia/Shanghai',
    'load-url'='test-starrocks:8035',
    'database-name' = 'tmp',
    'table-name' = 'dws_user_recharge_login_join_dim',
    'username' = 'root',
    'password' = '',
    'sink.buffer-flush.max-rows' = '1000000',
    'sink.buffer-flush.max-bytes' = '300000000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.column_separator' = '\x01',
    'sink.properties.row_delimiter' = '\x02',
    'sink.max-retries' = '3',
    'sink.properties.columns' = 'userId,window_end,gender,region,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time'
    )
;

INSERT INTO sink_sr_dws_user_recharge_login_join_dim
SELECT t1.userId,window_end,t2.gender,cast(t2.region as int) as region,login_num,first_login_time,last_login_time,recharge_num,total_recharge_amount,first_recharge_time,last_recharge_time
FROM view_dws_user_recharge_login t1 LEFT JOIN dim_user t2 on t1.userId=t2.rowkey
;