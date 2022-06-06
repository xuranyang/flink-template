package com.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
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

        DataStreamSource<String> dataStreamSource = env.fromElements("hello,2022-01-01", "world,2022-01-02", "flink,2022-01-01");
        String path = "hdfs://master01:8020/tmp/";

        DateTimeBucketAssigner<String> bucketAssigner = new DateTimeBucketAssigner<>(
//                "yyyy-MM-dd--HH-mm",
                "yyyy-MM-dd",
                ZoneId.of("Asia/Shanghai"));

        // 自定义分区路径
        BucketAssigner<String, String> udfDtBucketAssigner = new BucketAssigner<String, String>() {
            @Override
            public String getBucketId(String element, Context context) {
                String dt = element.split(",")[1];
                return dt;
            }

            @Override
            public SimpleVersionedSerializer<String> getSerializer() {
                return SimpleVersionedStringSerializer.INSTANCE;
            }
        };

        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                // 滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.create()
                                // 滚动写入新文件的时间，默认60s。
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 默认60s空闲，就滚动写入新的文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // 设置每个文件的最大大小 ,默认是128M
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
                // 分桶策略
//                .withBucketAssigner(bucketAssigner)
                .withBucketAssigner(udfDtBucketAssigner)
                .withOutputFileConfig(OutputFileConfig.builder()
                        // 设置文件前缀
                        .withPartPrefix("prefix")
                        // 设置文件后缀
                        .withPartSuffix(".end")
                        .build())
                .build();

        dataStreamSource.addSink(streamingFileSink);

        env.execute();
    }
}
