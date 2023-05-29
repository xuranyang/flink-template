package com.schema;

import com.alibaba.fastjson.JSONObject;
import com.model.KafkaRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KafkaRecordSchema implements KafkaRecordDeserializationSchema<KafkaRecord> {
    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaRecord> collector) throws IOException {
        // 如果key为null 则 直接赋值为null
        String key = consumerRecord.key() != null ? new String(consumerRecord.key(), UTF_8.name()) : null;
        String value = new String(consumerRecord.value(), UTF_8.name());
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        long timestamp = consumerRecord.timestamp();


        KafkaRecord kafkaRecord = KafkaRecord.builder()
                .kafkaTopic(consumerRecord.topic())
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .kafkaTimestamp(timestamp)
                .kafkaKey(key)
                .kafkaValue(value)
                .build();
        collector.collect(kafkaRecord);
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
//        return null;
        return TypeInformation.of(KafkaRecord.class);
    }
}
