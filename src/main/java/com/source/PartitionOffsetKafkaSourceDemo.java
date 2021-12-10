package com.source;

import com.sink.util.PropUtil;
import com.source.util.PartitionOffsetKafkaStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class PartitionOffsetKafkaSourceDemo {

    public static PropUtil propUtil = new PropUtil();

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // -topic topicName
        String topic = parameterTool.get("topic");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //设置checkpoint时间
//        env.enableCheckpointing(600000);
        env.enableCheckpointing(1000);

        final Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", propUtil.getStringProp("kafka.brokers"));  //多个的话可以指定
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /**
         * auto.offset.reset 指定kafka消费者从哪里开始消费数据
         * earliest：当各分区下有已提交的offset时，从提交的offset开始消费,无提交的offset时,从头开始消费
         * latest：当各分区下有已提交的offset时，从提交的offset开始消费,无提交的offset时,消费新产生的该分区下的数据
         * none：topic各分区都存在已提交的offset时，从offset后开始消费，只要有一个分区不存在已提交的offset，则抛出异常
         */
//        properties.setProperty("auto.offset.reset","latest");
//        properties.setProperty("auto.offset.reset","earliest");
//        properties.setProperty("auto.offset.reset","none");


        /**
         * 多个消费者组 group.id 要不同
         */
        properties.setProperty("group.id", "test_group");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new PartitionOffsetKafkaStringSchema(), properties);

        /**
         * setStartFromEarliest():最早的数据开始消费,该模式下Kafka中的 committed offset 将被忽略,不会用作起始位置
         * setStartFromLatest():最新的数据开始消费,该模式下Kafka中的 committed offset 将被忽略,不会用作起始位置
         *
         * setStartFromGroupOffsets():消费者组最近一次提交的偏移量，默认。
         * 如果找不到分区的偏移量，那么将会使用配置中的 auto.offset.reset 设置。
         * 任务从检查点重启，按照重启前的offset进行消费;
         * 如果直接重启不从检查点重启并且group.id不变，程序会按照上次提交的offset的位置继续消费。
         *
         * setStartFromTimestamp():指定具体的偏移量时间戳,毫秒
         * 对于每个分区，其时间戳大于或等于指定时间戳的记录讲用作起始位置。
         * 如果一个分区的最新记录早于指定的时间戳,则只从最新记录读取该分区数据。
         * 在这种模式下,kafka中的已提交 offset 将被忽略,不会用作起始位置。
         */
//        flinkKafkaConsumer.setStartFromEarliest();
//        flinkKafkaConsumer.setStartFromLatest();
        flinkKafkaConsumer.setStartFromGroupOffsets();
//        flinkKafkaConsumer.setStartFromTimestamp(System.currentTimeMillis());

        /**
         * 为每个分区指定偏移量
         */
//        Map<KafkaTopicPartition, Long> specificOffsetsMap = new HashMap<>();
//        specificOffsetsMap.put(new KafkaTopicPartition(topic, 0), 8L);
//        specificOffsetsMap.put(new KafkaTopicPartition(topic, 1), 24L);
//        flinkKafkaConsumer.setStartFromSpecificOffsets(specificOffsetsMap);

        /**
         * 如果禁用CheckPointing，则Flink Kafka Consumer依赖于内部使用的Kafka客户端的自动定期偏移量提交功能。
         * 该偏移量会被记录在 Kafka 中的 _consumer_offsets 这个特殊记录偏移量的 Topic 中。
         *
         * 如果启用CheckPointing，偏移量则会被记录在 StateBackend 中。该方法kafkaSource.setCommitOffsetsOnCheckpoints(boolean);
         * 设置为 ture 时，偏移量会在 StateBackend 和 Kafka 中的 _consumer_offsets Topic 中都会记录一份；
         * 设置为 false 时，偏移量只会在 StateBackend 中的 存储一份。
         *
         * 如果启用CheckPointing,无论设置 true or false(默认为 true)，flink都是主动管理,
         * 如果是true,flink会在完成 checkpoint 以后,把 offset 提交给kafka
         *
         * 如果是false,即 启用了 checkpoint，但是禁用 CommitOffsetsOnCheckpoints， kafka 消费者组的 offset 不会提交到 kafka，
         * 也就是说： 消费者组的 offset 是不会有变化的!
         */
        // 设置checkpoint后再提交offset,即oncheckpoint模式,默认为true
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer);

        dataStreamSource.print();
//        dataStreamSource.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                JSONObject s1 = JSONObject.parseObject(s);
//                Integer partition = s1.getInteger("partition");
//                Long offset = s1.getLong("offset");
//                JSONObject record = s1.getJSONObject("record");
//                String id = record.getString("id");
//
//                return String.format("[Record][Partition:%s][Offset:%s][id:%s]", partition, offset, id);
//            }
//        }).print();

        env.execute();
    }
}
