package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KafkaRecord {
    private String kafkaTopic;
    private Integer kafkaPartition;
    private Long kafkaOffset;
//    private T kafkaKey;
    private String kafkaKey;
    private String kafkaValue;
    private Long kafkaTimestamp;
}
