package com.util.hbase;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HbaseProperties {
    private String zkQuorum;

    // hbase 在hdfs上面存储数据的目录，需要完整的路径，例如：hdfs://hbase01:9000/hbase 这种形式，极其重要
    private String rootDir;

    private Integer zkPort = 2181;

    // zk 中的HBASE的根Zode,默认为/hbase
    private String zNodeParent = "/hbase";

    // 重试次数，默认为 10，可配置为 3
    private Integer retryNum = 3;

//    private Integer timeoutMs = 1000;

    // 自定义的Hbase是否使用线程池,默认不使用线程池
    private Boolean useThreadPool = false;
    // 自定义的Hbase线程池大小,在使用线程池时有用
    private Integer poolSize = 3;
}
