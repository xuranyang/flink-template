package com.sql.new_version.hive;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaToHiveDemoProduceMessage {
    public static void main(String[] args) throws Exception {
        // 生产2批数据 每批10条记录 每次间隔10s,触发watermark
        int batch = 2;
        for (int i = 0; i < batch; i++) {
            // 先生产Kafka数据
            produceKafkaMessage(10);
            TimeUnit.SECONDS.sleep(10);
        }

    }


    /**
     * 生产 Kafka 测试数据
     *
     * @param num
     */
    public static void produceKafkaMessage(int num) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka01:9092");
        props.put("acks", "all");
        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");       // 键的序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  // 值的序列化

        //生产者发送消息
        String topic = "HIVE_TEST_1";
        Producer<String, String> procuder = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < num; i++) {
            String uuid = UUID.randomUUID().toString();
            Map<String, Object> map = Maps.newHashMap();
            map.put("user_id", new Random().nextInt(100));
            map.put("order_amount", new Random().nextDouble() * 1000);
            map.put("log_ts", sdf.format(new Date()));
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, uuid, JSONObject.toJSONString(map));
            procuder.send(record);
        }

        System.out.println("消息发送完成。");

        procuder.close(); // 主动关闭生产者
    }
}
