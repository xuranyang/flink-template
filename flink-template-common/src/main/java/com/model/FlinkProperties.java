package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
//@Accessors(chain = true)
public class FlinkProperties {
    private Integer parallelism;
    private Long chk;
    private String envType;
    private String kafkaTopic;
    private String kafkaBrokers;
    private String kafkaGroupId;
    private String kafkaGroupIdSuffix;
    private String globalStrategy;
    private Long globalTimestamp = -1L;
    private String globalTimeZone;
    private String mode = "STREAMING";
}
