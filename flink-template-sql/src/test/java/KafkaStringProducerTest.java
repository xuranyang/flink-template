import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaStringProducerTest {
    private static Producer<String, String> procuder;

    public static void main(String[] args) throws Exception {
        initKafkaProducer();

//        sendKafkaMessage("CANAL_TEST_1", "flink-template-sql/src/main/java/com/data/user_login.txt");
//        sendKafkaMessage("CANAL_TEST_2", "flink-template-sql/src/main/java/com/data/user_recharge.txt");
        sendKafkaMessage("CANAL_TEST_1", "flink-template-sql/src/main/java/com/data/tmp1.txt");
        sendKafkaMessage("CANAL_TEST_2", "flink-template-sql/src/main/java/com/data/tmp2.txt");

        closeKafkaProducer();
    }

    public static void sendKafkaMessage(String topic, String kafkaFilePath) throws IOException {
        File jsonFile = new File(kafkaFilePath);
        List<String> jsonStrList = FileUtils.readLines(jsonFile, "UTF-8");

        for (String jsonStr : jsonStrList) {
//            System.out.println(jsonStr);
            String uuid = UUID.randomUUID().toString();
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, uuid, jsonStr);
            procuder.send(msg);
        }

    }


    public static void initKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "test-kafka:9092");
//        props.put("bootstrap.servers", "172.28.32.144:9092,172.28.33.85:9092,172.28.34.26:9092");
        props.put("acks", "all");
        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");       // 键的序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  // 值的序列化

        //生产者发送消息
        procuder = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    public static void closeKafkaProducer() {
        System.out.println("消息发送完成。");
        procuder.close(); // 主动关闭生产者
    }
}
