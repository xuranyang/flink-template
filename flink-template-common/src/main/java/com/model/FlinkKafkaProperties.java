package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Accessors(chain = true)
public class FlinkKafkaProperties {
    private String kafkaTopics;
    private String kafkaBrokers;
    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private String kafkaGroupIdSuffix;
    private String kafkaStrategy;
    private Long kafkaTimestamp = -1L;
    private String kafkaTimeZone;
    private String kafkaOffsets;
}
