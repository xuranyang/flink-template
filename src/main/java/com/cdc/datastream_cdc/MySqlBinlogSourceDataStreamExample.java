package com.cdc.datastream_cdc;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class MySqlBinlogSourceDataStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use parallelism 1 for sink to keep message ordering
        env.setParallelism(1);

        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/flink/cdc/ck"));

        // 最多重启3次,每次重启间隔2秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));

        // 设置   cancel任务时,保留最后一次的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME","hive");

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test_db") // 没有tableList时,监控该库下的所有表
                .tableList("test_db.test_table")
                // 全量读取当前快照,然后以当前快照为基准,增量读取binlog
                .startupOptions(StartupOptions.initial())
                // 从最早的binlog开始读取
                .startupOptions(StartupOptions.earliest())
                // 从最新的binlog开始读取
                .startupOptions(StartupOptions.latest())
                // 从debezium指定位点继续读取binlog,断点续传
                .startupOptions(StartupOptions.timestamp(1000L))
                // 从指定的binlog的时间戳开始读取
                .startupOptions(StartupOptions.specificOffset("specificOffsetFile",123456789))
                // .debeziumProperties(new Properties())
                // 反序列化
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();




        env.addSource(sourceFunction).print();

        env.execute();
    }
}
