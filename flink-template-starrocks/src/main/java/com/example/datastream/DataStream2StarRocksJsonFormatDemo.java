package com.example.datastream;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
CREATE TABLE tmp.test_table_json_uk(
  `id` bigint,
  `regTime` datetime,
  `name` string,
  `age` int,
  `type` string
)
UNIQUE KEY (id,regTime)
PARTITION BY RANGE(regTime)()
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-365",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

CREATE TABLE tmp.test_table_json_pk(
  `id` bigint,
  `regTime` datetime,
  `name` string,
  `age` int,
  `type` string
)
PRIMARY KEY (id,regTime)
PARTITION BY RANGE(regTime)()
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-365",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

ALTER TABLE tmp.test_table_json_uk SET("dynamic_partition.enable"="false");
ALTER TABLE tmp.test_table_json_uk ADD PARTITIONS START ("2022-06-01") END ("2022-06-04") EVERY (interval 1 day);
ALTER TABLE tmp.test_table_json_uk SET("dynamic_partition.enable"="true");

ALTER TABLE tmp.test_table_json_pk SET("dynamic_partition.enable"="false");
ALTER TABLE tmp.test_table_json_pk ADD PARTITIONS START ("2022-06-01") END ("2022-06-04") EVERY (interval 1 day);
ALTER TABLE tmp.test_table_json_pk SET("dynamic_partition.enable"="true");
*/

/**
 * Flink StarRocksSink 使用json格式的StreamLoad,避免使用 默认csv格式的StreamLoad时 出现与分隔符相同的数据而导致写入失败
 */
public class DataStream2StarRocksJsonFormatDemo {
    private static final String INSERT = "INSERT";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";

    private static final String OP = "__op";
    private static final Integer UPSERT_OP = 0;
    private static final Integer DELETE_OP = 1;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        RowData[] rowData = {
                new RowData(1L, "Maybe", 26, "2022-06-01", INSERT)
                , new RowData(2L, "Ame", 24, "2022-06-02", UPDATE)
                , new RowData(3L, "Ana", 21, "2022-06-03", INSERT)
                , new RowData(3L, "Ana", 21, "2022-06-03", DELETE)
        };

        DataStreamSource<RowData> rowDataDataStreamSource = env.fromElements(rowData);

        String database = "tmp";
        String table = "test_table_json_uk";

        boolean pkModel = parameterTool.getBoolean("pkModel", true);
        if (pkModel) {
            table = "test_table_json_pk";
        }

        StarRocksSinkOptions starRocksSinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "123456")
                .withProperty("database-name", database)
                .withProperty("table-name", table)
                .withProperty("sink.buffer-flush.max-rows", "1000000")
                .withProperty("sink.buffer-flush.max-bytes", "300000000")
                .withProperty("sink.buffer-flush.interval-ms", "10000")
                .withProperty("sink.max-retries", "3")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();


        DataStream<String> dataStream = rowDataDataStreamSource.map(rowDataObj -> {
            String s = JSONObject.toJSONString(rowDataObj);
            JSONObject jsonObject = JSONObject.parseObject(s);
            if (DELETE.equals(rowDataObj.getType())) {
                jsonObject.put(OP, DELETE_OP);
            }
            return JSONObject.toJSONString(jsonObject);
        });

        dataStream.addSink(StarRocksSink.sink(starRocksSinkOptions));

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    private static class RowData {
        private Long id;
        private String name;
        private int age;
        private String regTime;
        private String type;
    }

}
