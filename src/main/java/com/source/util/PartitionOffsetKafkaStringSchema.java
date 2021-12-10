package com.source.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PartitionOffsetKafkaStringSchema implements KafkaDeserializationSchema<String> {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        Map<String, String> kafkaConsumeMap = new HashMap<>();

        String value = new String(consumerRecord.value(), UTF_8.name());
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();

        kafkaConsumeMap.put("record", value);
        kafkaConsumeMap.put("offset", String.valueOf(offset));
        kafkaConsumeMap.put("partition", String.valueOf(partition));
        return JSONObject.toJSONString(kafkaConsumeMap);
    }

    @Override
    public TypeInformation<String> getProducedType() {
//        return null;
        return TypeInformation.of(String.class);
    }
}
