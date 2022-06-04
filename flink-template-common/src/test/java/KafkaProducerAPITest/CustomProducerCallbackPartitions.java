package KafkaProducerAPITest;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者发送消息的分区策略
 * 默认的分区器 DefaultPartitioner
 * (1)指明partition的情况下，直接将指明的值作为partition值;例如partition=0，所有数据写入分区0
 * (2)没有指明partition值但有key的情况下，将key的hash值与topic的 partition数进行取余得到partition值;
 * 例如: key1的hash值=5, key2的hash值=6, topic的partition数=2,那么key1 对应的value1写入1号分区, key2对应的value2写入0号分区。
 * (3)既没有partition值又没有key值的情况下，Kafka采用Sticky Partition（黏性分区器）,
 * 会随机选择一个分区，并尽可能一直使用该分区，待该分区的batch已满或者已完成，Kafka再随机一个分区进行使用（和上一次的分区不同）。
 * 例如：第一次随机选择0号分区，等0号分区当前批次满了（默认16k）或者linger.ms设置的时间到， Kafka再随机一个分区进行使用（如果还是0会继续随机）。
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2. 给 kafka 配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01:9092");
        // key,value 序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "MyPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        /**
         * 将数据发往指定 partition 的情况下，例如，将所有数据发往分区 1 中。
         */
        for (int i = 0; i < 5; i++) {
            // 指定数据发送到 1 号分区，key 为空（IDEA 中 ctrl + p 查看参数）
            kafkaProducer.send(new ProducerRecord<>("topic_first", 1, "", "kafka_value " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        /**
         * 没有指明 partition 值但有 key 的情况下，将 key 的 hash 值与 topic 的 partition 数进行取余得到 partition 值。
         */
        for (int i = 0; i < 5; i++) {
            // 依次指定 key 值为 a,b,f, 数据 key 的 hash 值与 3 个分区求余,分别发往分区 1、2、0
            kafkaProducer.send(new ProducerRecord<>("topic_first", "a", "kafka_value " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }


        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("topic_first", "kafka_value " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println(" 主题： " + metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        kafkaProducer.close();
    }

}
