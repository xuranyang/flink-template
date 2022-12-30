package com.cdc.datastream_cdc;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/about.html
 */
public class FlinkCdcDemo {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flink_test_db") // set captured database
                .tableList("flink_test_db.cdc_test_table") // set captured table
                .username("root")
                .password("123456")
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        //3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //4.打印数据
        mysqlDS.setParallelism(4).print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
