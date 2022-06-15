package com.example.datastream;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

/*
CREATE TABLE tmp.test_table_uk(
  `id` bigint,
  `reg_time` datetime,
  `name` string,
  `age` int,
  `type` string
)
UNIQUE KEY (id,reg_time)
PARTITION BY RANGE(reg_time)()
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-3",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

CREATE TABLE tmp.test_table_pk(
  `id` bigint,
  `reg_time` datetime,
  `name` string,
  `age` int,
  `type` string
)
PRIMARY KEY (id,reg_time)
PARTITION BY RANGE(reg_time)()
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-3",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "8",
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

*/
public class DataStream2StarRocksDemo {
    enum OP {
        INSERT, UPDATE, DELETE
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        RowData[] rowData = {
                new RowData(1L, "Maybe", 26, "2022-06-01", OP.INSERT)
                , new RowData(2L, "Ame", 24, "2022-06-02", OP.UPDATE)
                , new RowData(3L, "Ana", 21, "2022-06-03", OP.INSERT)
                , new RowData(3L, "Ana", 21, "2022-06-03", OP.DELETE)
        };

        DataStreamSource<RowData> rowDataDataStreamSource = env.fromElements(rowData);

        String database = "tmp";
        String table = "test_table_uk";

        /**
         * 是否为主键模型
         */
//        boolean pkModel = true;
//        boolean pkModel = false;
        boolean pkModel = parameterTool.getBoolean("pkModel", true);

        TableSchema.Builder builder = TableSchema.builder();

        if (pkModel) {
            table = "test_table_pk";
            // 要和 StarRocks 表的 PRIMARY KEY 的定义保持一致
            builder.primaryKey("id", "reg_time");
        }

        TableSchema tableSchema = builder
                .field("id", DataTypes.BIGINT().notNull())
                .field("reg_time", DataTypes.STRING().notNull())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("type", DataTypes.STRING())
                .build();

        StarRocksSinkOptions starRocksSinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "123456")
                .withProperty("database-name", database)
                .withProperty("table-name", table)
                .withProperty("sink.properties.column_separator", "\\x01")
                .withProperty("sink.properties.row_delimiter", "\\x02")
                .withProperty("sink.buffer-flush.max-rows", "1000000")
                .withProperty("sink.buffer-flush.max-bytes", "300000000")
                .withProperty("sink.buffer-flush.interval-ms", "10000")
                .withProperty("sink.max-retries", "3")
                .build();


        rowDataDataStreamSource.addSink(
                StarRocksSink.sink(
                        tableSchema, starRocksSinkOptions,
                        (slots, streamRowData) -> {
                            slots[0] = streamRowData.getId();
                            slots[1] = streamRowData.getRegTime();
                            slots[2] = streamRowData.getName();
                            slots[3] = streamRowData.getAge();
                            OP type = streamRowData.getType();
                            slots[4] = type.name();

                            // 如果是主键模型 需要额外增加一个 __op 的判断
                            if (pkModel) {
                                if (OP.INSERT.equals(type) || OP.UPDATE.equals(type)) {
                                    slots[5] = StarRocksSinkOP.UPSERT.ordinal();
                                } else if (OP.DELETE.equals(type)) {
                                    slots[5] = StarRocksSinkOP.DELETE.ordinal();
                                }
                            }

                        }
                )
        );

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
        private OP type;
    }

}
