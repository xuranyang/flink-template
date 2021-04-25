package com.source;

import com.sink.util.PropUtil;
import com.source.util.UserDefineKafkaDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KakfaSourceDemo {
    public static void main(String[] args) throws Exception {
        String topic = "Kafka-Source-Topic";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        /**
         * 必要参数 Kafka地址和消费者组名
         */
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        /**
         * 非必要参数
         */
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");

        /**
         * 简易获取Kafka数据信息
         */
//        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties).setStartFromGroupOffsets());

        /**
         * 自定义额外同时获取Kafka分区信息和位点信息
         */
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(topic, new UserDefineKafkaDeserializationSchema(), properties).setStartFromLatest());

        dataStreamSource.print("KafkaSourceData");

        // KafkaSink
//        dataStreamSource.addSink(new FlinkKafkaProducer<String>("remotehost:9092", "remote_topic_name", new SimpleStringSchema()));

        env.execute();
    }
}
