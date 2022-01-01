package com.source.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;

/**
 * 自定义
 * Flink KafkaSource 读取Kafka数据时,同时获取分区和位点信息
 */
public class UserDefineKafkaDeserializationSchema implements KafkaDeserializationSchema<String> {

    public static final Charset UTF_8 = Charset.forName("UTF-8");

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String value = new String(consumerRecord.value(), UTF_8.name());
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        JSONObject jsonObject = JSONObject.parseObject(value);
        jsonObject.put("offset", offset);
        jsonObject.put("partition", partition);
        return jsonObject.toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
//        return null;
    }
}
