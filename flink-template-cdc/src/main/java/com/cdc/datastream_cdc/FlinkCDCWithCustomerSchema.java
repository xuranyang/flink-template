package com.cdc.datastream_cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.Properties;

/**
 * 自定义反序列化器
 */
public class FlinkCDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建Flink-MySQL-CDC的Source
        Properties properties = new Properties();

        properties.setProperty("debezium.snapshot.mode", "initial");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("master01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test_db")
                .tableList("test_db.test_table")         //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据,注意：指定的时候需要使用"db.table"的方式
                .debeziumProperties(properties)
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new DebeziumDeserializationSchema<String>() { //自定义数据解析器
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        //获取主题信息,包含着数据库和表名  mysql_binlog_source.test_db.test_table
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        //创建JSON对象用于存储数据信息
                        JSONObject data = new JSONObject();
                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o);
                        }

                        //创建JSON对象用于封装最终返回值数据信息
                        JSONObject result = new JSONObject();
                        result.put("operation", operation.toString().toLowerCase());
                        result.put("data", data);
                        result.put("database", db);
                        result.put("table", tableName);

                        //发送数据至下游
                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
//                        return null;
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        //3.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //4.打印数据
        mysqlDS.print();

        //5.执行任务
        env.execute();
    }
}
