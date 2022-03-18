package com.sink;

import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkKafkaProducerByKeyDemo {
    public static String KAFKA_BROKERS = "127.0.0.1:9092";
    public static String KAFKA_ZK_CONNECT = "127.0.0.1:2181";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "actual_kafka_topic";
        DataStreamSource<String> dataStreamSource = env.fromElements("c:maybe", "a:fy", "b:ame", "d:chalice", "a:xnova", "b:ana");

        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.setProperty("bootstrap.servers", KAFKA_BROKERS);
        properties.setProperty("zookeeper.connect", KAFKA_ZK_CONNECT);
        properties.setProperty("group.id", "test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

//        dataStreamSource.print();
        dataStreamSource.addSink(new FlinkKafkaProducer<String>("default_topic",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                        byte[] key = s.split(":")[0].getBytes();    // kafka key
                        byte[] value = s.split(":")[1].getBytes();  // kafka value
                        return new ProducerRecord<byte[], byte[]>(topic, key, value);
                    }
                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute();
    }
}
