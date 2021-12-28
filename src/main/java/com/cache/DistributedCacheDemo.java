package com.cache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistributedCacheDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env.registerCachedFile("src/main/java/com/cache/cache.txt", "cache_file");

        DataStreamSource<String> dataStreamSource = env.fromCollection(Arrays.asList("a", "b", "c"));

        dataStreamSource.map(new RichMapFunction<String, String>() {
            private List<String> cacheList = new ArrayList<>();
            private String cacheInfo;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File cache_file = getRuntimeContext().getDistributedCache().getFile("cache_file");
                cacheList = FileUtils.readLines(cache_file);
                cacheInfo = FileUtils.readFileToString(cache_file, "utf-8");
            }

            @Override
            public String map(String value) throws Exception {
                System.out.println(value + ":" + cacheList);
                return value + ":" + cacheInfo;
            }
        }).print();

        env.execute();
    }
}
