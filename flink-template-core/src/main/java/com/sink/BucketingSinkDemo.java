package com.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.hadoop.fs.Path;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.9/dev/connectors/filesystem_sink.html
 * BucketingSink 自 Flink 1.9 起已被弃用，并将在后续版本中删除。请改用 StreamingFileSink。
 * 以下代码用1.9版本可以成功执行,用1.13版本执行会报错
 */
public class BucketingSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.fromElements("hello,2022-01-01", "world,2022-01-02", "flink,2022-01-01");
        String path = "hdfs://master01:8020/tmp";
        System.setProperty("HADOOP_USER_NAME", "hive");

        BucketingSink<String> bucketingSink = new BucketingSink<>(path);
        bucketingSink.setBucketer(new BasePathBucketer<String>() {
            @Override
            public Path getBucketPath(Clock clock, Path basePath, String element) {
                try {
                    String dt = element.split(",")[1].substring(0, 10);
                    return new Path(basePath + "/" + dt);
                } catch (Exception e) {
//                    log.info("[Split Dt Error]:{}", element);
                    return new Path(basePath + "/1970-01-01");
                }
            }
        });

        // 设置文件块大小128M，超过128M会关闭当前文件，开启下一个文件
        bucketingSink.setBatchSize(1024 * 1024 * 128L);
        // 设置10分钟翻滚一次
        bucketingSink.setBatchRolloverInterval(10 * 60 * 1000L);
        // 设置等待写入的文件前缀,默认是_
        bucketingSink.setPendingPrefix("_.");
        // 设置等待写入的文件后缀,默认是.pending
        bucketingSink.setPendingSuffix(".pending");
        //设置正在处理的文件前缀,默认为_
        bucketingSink.setInProgressPrefix("_.");
        //设置正在处理的文件后缀,默认为.in-progress
        bucketingSink.setInProgressSuffix(".in-progress");
        // 文件前缀,默认为part
        bucketingSink.setPartPrefix("test_file_name");
        // 文件后缀,默认为partnull
        bucketingSink.setPartSuffix("_end");


        dataStreamSource.addSink(bucketingSink);

        env.execute();
    }
}
