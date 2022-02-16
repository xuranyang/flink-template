package com.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.9/dev/connectors/streamfile_sink.html
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/connectors/streamfile_sink.html
 */
public class StreamingFileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.fromElements("hello", "world", "flink");
        String path = "hdfs://master01:8020/tmp/";

        DateTimeBucketAssigner<String> bucketAssigner = new DateTimeBucketAssigner<>(
//                "yyyy-MM-dd--HH-mm",
                "yyyy-MM-dd",
                ZoneId.of("Asia/Shanghai"));

        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.create()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withBucketAssigner(bucketAssigner)
                .build();

        dataStreamSource.addSink(streamingFileSink);

        env.execute();
    }
}
