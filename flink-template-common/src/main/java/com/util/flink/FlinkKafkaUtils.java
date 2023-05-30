package com.util.flink;

import com.constant.PropertiesConstants;
import com.enums.KafkaResetStrategyEnum;
import com.google.common.collect.Lists;
import com.model.FlinkKafkaProperties;
import com.model.FlinkProperties;
import com.schema.KafkaRecordSchema;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/
 */
public class FlinkKafkaUtils {
    private static Logger log = LoggerFactory.getLogger(FlinkKafkaUtils.class);

    public static Properties getKafkaCommonProperties() {
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("request.timeout.ms", "180000"); //默认 30000
        return properties;
    }

    /**
     * 1.
     * partition:offset
     * 0:15
     * 0:15,1:10
     * <p>
     * 2.
     * topic:partition:offset
     * topic1:0:15,topic2:0:20
     *
     * @param kafkaTopicsList
     * @param kafkaOffsets
     * @return
     */
    public static Map<TopicPartition, Long> getKafkaTopicPartitionOffsetMap(List<String> kafkaTopicsList, String kafkaOffsets) {
        Map<TopicPartition, Long> kafkaTopicPartitionOffsetMap;
        if (kafkaTopicsList.size() == 1) {
            kafkaTopicPartitionOffsetMap = getKafkaTopicPartitionOffsetMap(kafkaTopicsList.get(0), kafkaOffsets);
        } else {
            kafkaTopicPartitionOffsetMap = getKafkaTopicPartitionOffsetMap(kafkaOffsets);
        }
        return kafkaTopicPartitionOffsetMap;
    }

    /**
     * @param topic
     * @param partitionOffsets
     * @return
     */
    public static Map<TopicPartition, Long> getKafkaTopicPartitionOffsetMap(String topic, String partitionOffsets) {
        Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<>();

        for (String partitionOffset : partitionOffsets.split(PropertiesConstants.SPLIT_COMMA)) {
            int kafkaPartition = Integer.parseInt(partitionOffset.split(PropertiesConstants.SPLIT_COLON)[0]);
            Long kafkaOffset = Long.valueOf(partitionOffset.split(PropertiesConstants.SPLIT_COLON)[1]);

            TopicPartition topicPartition = new TopicPartition(topic, kafkaPartition);
            topicPartitionOffsetMap.put(topicPartition, kafkaOffset);
        }

        return topicPartitionOffsetMap;
    }

    /**
     * @param topicPartitionOffsets
     * @return
     */
    public static Map<TopicPartition, Long> getKafkaTopicPartitionOffsetMap(String topicPartitionOffsets) {
        Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<>();

        for (String topicPartitionOffset : topicPartitionOffsets.split(PropertiesConstants.SPLIT_COMMA)) {
            String topic = topicPartitionOffset.split(PropertiesConstants.SPLIT_COLON)[0];
            int kafkaPartition = Integer.parseInt(topicPartitionOffset.split(PropertiesConstants.SPLIT_COLON)[1]);
            Long kafkaOffset = Long.valueOf(topicPartitionOffset.split(PropertiesConstants.SPLIT_COLON)[2]);

            TopicPartition topicPartition = new TopicPartition(topic, kafkaPartition);
            topicPartitionOffsetMap.put(topicPartition, kafkaOffset);
        }

        return topicPartitionOffsetMap;
    }

    public static List<String> getKafkaTopicsList(String kafkaTopics) {
        List<String> kafkaTopicsList = Arrays.asList(kafkaTopics.split(PropertiesConstants.SPLIT_COMMA));
        return kafkaTopicsList;
    }

    public static List<String> getKafkaTopicsList(List<String> kafkaTopics) {
        return kafkaTopics;
    }

    public static DataStreamSource<String> getKafkaSource(StreamExecutionEnvironment env, FlinkProperties flinkProperties) {
        KafkaRecordDeserializationSchema<String> stringKafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class);
        return getKafkaSource(env, flinkProperties, stringKafkaRecordDeserializationSchema);
    }

    public static <T> DataStreamSource<T> getKafkaSource(StreamExecutionEnvironment env, FlinkProperties flinkProperties, KafkaRecordDeserializationSchema<T> kafkaRecordDeserializationSchema) {
        FlinkKafkaProperties flinkKafkaProperties = FlinkKafkaProperties.builder()
                .kafkaTopics(flinkProperties.getKafkaTopics())
                .kafkaBrokers(flinkProperties.getKafkaBrokers())
                .kafkaBootstrapServers(flinkProperties.getKafkaBootstrapServers())
                .kafkaGroupId(flinkProperties.getKafkaGroupId())
                .kafkaGroupIdSuffix(flinkProperties.getKafkaGroupIdSuffix())
                .kafkaStrategy(flinkProperties.getGlobalStrategy())
                .kafkaTimestamp(flinkProperties.getGlobalTimestamp())
                .kafkaTimeZone(flinkProperties.getGlobalTimeZone())
                .kafkaOffsets(flinkProperties.getKafkaOffsets())
                .build();

        DataStreamSource<T> dataStreamSource = getKafkaSource(env, flinkKafkaProperties, kafkaRecordDeserializationSchema);

        return dataStreamSource;
    }

    public static DataStreamSource<String> getKafkaSource(StreamExecutionEnvironment env, FlinkKafkaProperties flinkKafkaProperties) {
        KafkaRecordDeserializationSchema<String> stringKafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class);
        return getKafkaSource(env, flinkKafkaProperties, stringKafkaRecordDeserializationSchema);
    }

    public static <T> DataStreamSource<T> getKafkaSource(StreamExecutionEnvironment env, FlinkKafkaProperties flinkKafkaProperties, KafkaRecordDeserializationSchema<T> kafkaRecordDeserializationSchema) {
        KafkaSourceBuilder<T> kafkaSourceBuilder = KafkaSource.builder();
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        List<String> kafkaTopicsList = getKafkaTopicsList(flinkKafkaProperties.getKafkaTopics());

        // 通过 setProperties 设置属性
        kafkaSourceBuilder.setProperties(getKafkaCommonProperties());

        kafkaSourceBuilder.setTopics(kafkaTopicsList);
        kafkaSourceBuilder.setGroupId(flinkKafkaProperties.getKafkaGroupId());

        if (StringUtils.isNotBlank(flinkKafkaProperties.getKafkaBrokers())) {
            kafkaSourceBuilder.setBootstrapServers(parameterTool.get(flinkKafkaProperties.getKafkaBrokers()));
        } else {
            kafkaSourceBuilder.setBootstrapServers(flinkKafkaProperties.getKafkaBootstrapServers());
        }

        kafkaSourceBuilder.setDeserializer(kafkaRecordDeserializationSchema);
//        kafkaSourceBuilder.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));
//        kafkaSourceBuilder.setValueOnlyDeserializer(new SimpleStringSchema());

        kafkaSourceBuilderSetStrategy(kafkaSourceBuilder, flinkKafkaProperties);

        KafkaSource<T> kafkaSource = kafkaSourceBuilder.build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
    }

    /**
     * @param kafkaSourceBuilder
     * @param kafkaTopicsList
     * @param kafkaOffsets
     * @param <T>
     */
    public static <T> void kafkaSourceBuilderSetSpecialOffsets(KafkaSourceBuilder<T> kafkaSourceBuilder, List<String> kafkaTopicsList, String kafkaOffsets) {
        Map<TopicPartition, Long> kafkaTopicPartitionOffsetMap = getKafkaTopicPartitionOffsetMap(kafkaTopicsList, kafkaOffsets);
        kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(kafkaTopicPartitionOffsetMap, OffsetResetStrategy.LATEST));
    }

    public static <T> void kafkaSourceBuilderSetStrategy(KafkaSourceBuilder<T> kafkaSourceBuilder, FlinkKafkaProperties flinkKafkaProperties) {
        List<String> kafkaTopicsList = getKafkaTopicsList(flinkKafkaProperties.getKafkaTopics());
        String kafkaStrategy = flinkKafkaProperties.getKafkaStrategy();
        // 设置Kafka的启动策略
        if (KafkaResetStrategyEnum.EARLIEST.getStrategy().equalsIgnoreCase(kafkaStrategy)) {
            // 从最早位点开始消费
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
        } else if (KafkaResetStrategyEnum.LATEST.getStrategy().equalsIgnoreCase(kafkaStrategy)) {
            // 从最末尾位点开始消费
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        } else if (KafkaResetStrategyEnum.OFFSET.getStrategy().equalsIgnoreCase(kafkaStrategy)) {
            // 从消费组提交的位点开始消费，不指定位点重置策略
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
            // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
//            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));
        } else if (KafkaResetStrategyEnum.TIMESTAMP.getStrategy().equalsIgnoreCase(kafkaStrategy)) {
            // 从时间戳大于等于指定时间的数据开始消费
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(flinkKafkaProperties.getKafkaTimestamp()));
        } else if (KafkaResetStrategyEnum.SPECIAL.getStrategy().equalsIgnoreCase(kafkaStrategy)) {
            kafkaSourceBuilderSetSpecialOffsets(kafkaSourceBuilder, kafkaTopicsList, flinkKafkaProperties.getKafkaOffsets());
        }
    }
}
