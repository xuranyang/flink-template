package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
//@Accessors(chain = true)
public class FlinkProperties {
    private Integer parallelism = 1;
    private Long chk;
    private String envType;
    private String kafkaTopics;
    private String kafkaBrokers;
    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private String kafkaGroupIdSuffix;
    private String globalStrategy;
    private Long globalTimestamp = -1L;
    private String globalTimeZone;
    private String mode = RuntimeExecutionMode.STREAMING.name();
    private String kafkaOffsets;
//    private Map<TopicPartition, Long> kafkaOffsets;
}
