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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FlinkKafkaUtils {
    private static Logger log = LoggerFactory.getLogger(FlinkKafkaUtils.class);

    public static Properties getKafkaCommonProperties() {
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("request.timeout.ms", "180000"); //默认 30000
        return properties;
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
        KafkaSourceBuilder<T> kafkaSourceBuilder = KafkaSource.builder();

        // 通过 setProperties 设置属性
        kafkaSourceBuilder.setProperties(getKafkaCommonProperties());

        kafkaSourceBuilder.setTopics(getKafkaTopicsList(flinkProperties.getKafkaTopics()));
        kafkaSourceBuilder.setGroupId(flinkProperties.getKafkaGroupId());
        kafkaSourceBuilder.setBootstrapServers(flinkProperties.getKafkaBootstrapServers());

        kafkaSourceBuilder.setDeserializer(kafkaRecordDeserializationSchema);
//        kafkaSourceBuilder.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));
//        kafkaSourceBuilder.setValueOnlyDeserializer(new SimpleStringSchema());

        // 设置Kafka的启动策略
        if (KafkaResetStrategyEnum.EARLIEST.getStrategy().equalsIgnoreCase(flinkProperties.getGlobalStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
        } else if (KafkaResetStrategyEnum.LATEST.getStrategy().equalsIgnoreCase(flinkProperties.getGlobalStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        } else if (KafkaResetStrategyEnum.OFFSET.getStrategy().equalsIgnoreCase(flinkProperties.getGlobalStrategy())) {
            // 从消费组提交的位点开始消费，不指定位点重置策略
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
        } else if (KafkaResetStrategyEnum.TIMESTAMP.getStrategy().equalsIgnoreCase(flinkProperties.getGlobalStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(flinkProperties.getGlobalTimestamp()));
        }


        KafkaSource<T> kafkaSource = kafkaSourceBuilder.build();
        DataStreamSource<T> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");

        return dataStreamSource;
    }

    public static DataStreamSource<String> getKafkaSource(StreamExecutionEnvironment env, FlinkKafkaProperties flinkKafkaProperties) {
        KafkaRecordDeserializationSchema<String> stringKafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class);
        return getKafkaSource(env, flinkKafkaProperties, stringKafkaRecordDeserializationSchema);
    }

    public static <T> DataStreamSource<T> getKafkaSource(StreamExecutionEnvironment env, FlinkKafkaProperties flinkKafkaProperties, KafkaRecordDeserializationSchema<T> kafkaRecordDeserializationSchema) {
        KafkaSourceBuilder<T> kafkaSourceBuilder = KafkaSource.builder();
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();

        // 通过 setProperties 设置属性
        kafkaSourceBuilder.setProperties(getKafkaCommonProperties());

        kafkaSourceBuilder.setTopics(getKafkaTopicsList(flinkKafkaProperties.getKafkaTopics()));
        kafkaSourceBuilder.setGroupId(flinkKafkaProperties.getKafkaGroupId());

        if (StringUtils.isNotBlank(flinkKafkaProperties.getKafkaBrokers())) {
            kafkaSourceBuilder.setBootstrapServers(parameterTool.get(flinkKafkaProperties.getKafkaBrokers()));
        } else {
            kafkaSourceBuilder.setBootstrapServers(flinkKafkaProperties.getKafkaBootstrapServers());
        }

        kafkaSourceBuilder.setDeserializer(kafkaRecordDeserializationSchema);
//        kafkaSourceBuilder.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));
//        kafkaSourceBuilder.setValueOnlyDeserializer(new SimpleStringSchema());

        // 设置Kafka的启动策略
        if (KafkaResetStrategyEnum.EARLIEST.getStrategy().equalsIgnoreCase(flinkKafkaProperties.getKafkaStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
        } else if (KafkaResetStrategyEnum.LATEST.getStrategy().equalsIgnoreCase(flinkKafkaProperties.getKafkaStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
        } else if (KafkaResetStrategyEnum.OFFSET.getStrategy().equalsIgnoreCase(flinkKafkaProperties.getKafkaStrategy())) {
            // 从消费组提交的位点开始消费，不指定位点重置策略
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets());
        } else if (KafkaResetStrategyEnum.TIMESTAMP.getStrategy().equalsIgnoreCase(flinkKafkaProperties.getKafkaStrategy())) {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(flinkKafkaProperties.getKafkaTimestamp()));
        }

        KafkaSource<T> kafkaSource = kafkaSourceBuilder.build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
    }
}
