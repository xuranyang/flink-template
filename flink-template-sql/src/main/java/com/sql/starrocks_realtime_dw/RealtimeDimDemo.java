package com.sql.starrocks_realtime_dw;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;


/**
 * create 'dim_user_test',{NAME=>'base_info',COMPRESSION=>'SNAPPY'}
 * put 'dim_user_test','1001','base_info:gender','male'
 * put 'dim_user_test','1001','base_info:region','0'
 * put 'dim_user_test','1002','base_info:gender','male'
 * put 'dim_user_test','1002','base_info:region','0'
 * put 'dim_user_test','1003','base_info:gender','female'
 * put 'dim_user_test','1003','base_info:region','1'
 *
 * CREATE TABLE dim_user(
 *     rowkey INT,
 *     base_info ROW<gender STRING,region INT>,
 *     PRIMARY KEY (rowkey) NOT ENFORCED
 * ) WITH (
 *     'connector' = 'hbase-2.2',
 *     'table-name' = 'dim_user_test',
 *     'zookeeper.quorum' = 'fat01:2181,fat02:2181,fat03:2181',
 *     'zookeeper.znode.parent' = '/hbase'
 * );
 */
public class RealtimeDimDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 声明为流任务
                //.inBatchMode() // 声明为批任务
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        TableConfig config = tableEnv.getConfig();
        config.setLocalTimeZone(ZoneId.of("GMT+02:00"));

        tableEnv.executeSql("CREATE TABLE dim_user(\n" +
                "    rowkey INT,\n" +
                "    base_info ROW<gender STRING,region STRING>,\n" +
                "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'dim_user_test',\n" +
                "    'zookeeper.quorum' = 'fat01:2181,fat02:2181,fat03:2181',\n" +
                "    'zookeeper.znode.parent' = '/hbase'\n" +
                ")");


        tableEnv.executeSql("SELECT rowkey as userId,base_info.gender,base_info.region FROM dim_user").print();

    }
}
