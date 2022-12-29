package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.util.Arrays;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/hybridsource/
 * https://max.book118.com/html/2022/0123/7105030156004063.shtm
 * https://blog.csdn.net/penriver/article/details/122377396
 */
public class HybridSourceDemo {
    public static void main(String[] args) throws Exception {
//        hybridSourceDemo();
        hybridSourceStartPositionForNextSourceDemo();

    }

    public static void hybridSourceDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSource<String> fileSource =
                FileSource.forRecordStreamFormat(new TextLineFormat(), Path.fromLocalFile(new File("flink-template-core/src/main/java/com/data/hs.txt"))).build();
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka01:9092")
                        .setGroupId("test-group")
                        .setTopics(Arrays.asList("CANAL_TEST_1"))
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build();
        HybridSource<String> hybridSource =
                HybridSource.builder(fileSource)
                        .addSource(kafkaSource)
                        .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(hybridSource, WatermarkStrategy.noWatermarks(), "HybridSource");
        dataStreamSource.returns(TypeInformation.of(String.class)).print("HybridSource Print");

        env.execute();
    }

    public static void hybridSourceStartPositionForNextSourceDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long switchTimestamp = System.currentTimeMillis() - 60 * 1000L; // derive from file input paths

        FileSource<String> fileSource =
                FileSource.forRecordStreamFormat(new TextLineFormat(), Path.fromLocalFile(new File("flink-template-core/src/main/java/com/data/hs.txt"))).build();
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka01:9092")
                        .setGroupId("test-group")
                        .setTopics(Arrays.asList("CANAL_TEST_1"))
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
//                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setStartingOffsets(OffsetsInitializer.timestamp(switchTimestamp + 1))
                        .build();
        HybridSource<String> hybridSource =
                HybridSource.builder(fileSource)
                        .addSource(kafkaSource)
                        .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(hybridSource, WatermarkStrategy.noWatermarks(), "HybridSource");
        dataStreamSource.returns(TypeInformation.of(String.class)).print("HybridSource Print");

        env.execute();
    }
}
