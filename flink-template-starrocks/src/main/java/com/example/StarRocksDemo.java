package com.example;

import com.starrocks.connector.flink.StarRocksSink;
//import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StarRocksDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        batchJsonSink(env);
//        batchCsvSink(env);
        sqlStreamSink(tableEnv);

//        env.execute();
    }

    public static void sqlStreamSink(StreamTableEnvironment tableEnv) {
        String sourceSql = "CREATE TABLE source_table (\n" +
                " name STRING,\n" +
                " score INT\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='2',\n" +
                " 'fields.score.min'='1',\n" +
                " 'fields.score.max'='100',\n" +
                " 'fields.name.length'='4'\n" +
                ")";

        String sinkSql = "CREATE TABLE sink_table(" +
                "name VARCHAR," +
                "score BIGINT" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai'," +
                "'load-url'='127.0.0.1:8030'," +
                "'database-name' = 'test'," +
                "'table-name' = 'flink_test'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'sink.buffer-flush.max-rows' = '1000000'," +
                "'sink.buffer-flush.max-bytes' = '300000000'," +
                "'sink.buffer-flush.interval-ms' = '5000'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'," +
                "'sink.max-retries' = '3'," +
                "'sink.properties.columns' = 'name, score'" +
                ")";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);

//        tableEnv.executeSql("select * from source").print();
        tableEnv.executeSql("insert into sink_table select name,score from source_table");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class RowData {
        public int score;
        public String name;
    }

    public static void batchJsonSink(StreamExecutionEnvironment env) {
        // -------- sink with raw json string stream --------
        DataStreamSource<String> dataStreamSource = env.fromElements("{\"score\": \"99\", \"name\": \"stephen\"}",
                "{\"score\": \"100\", \"name\": \"lebron\"}");
        /**
         * StarRocks DDL:
         * CREATE TABLE IF NOT EXISTS test.flink_test (
         *     name VARCHAR(255) NOT NULL COMMENT "name of event",
         *     score INT NOT NULL COMMENT "score of event"
         * )
         * DUPLICATE KEY(name,score)
         * DISTRIBUTED BY HASH(name) BUCKETS 8;
         */
        dataStreamSource.addSink(StarRocksSink.sink(
                // the sink options
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai")
                        .withProperty("load-url", "127.0.0.1:8030")
                        .withProperty("username", "root")
                        .withProperty("password", "123456")
                        .withProperty("table-name", "flink_test")
                        .withProperty("database-name", "test")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .build()
        ));
        System.out.println("Sink Success");
    }

    public static void batchCsvSink(StreamExecutionEnvironment env) {
        // -------- sink with stream transformation --------
        DataStreamSource<RowData> rowDataDataStreamSource = env.fromElements(
                new RowData(101, "kobe"),
                new RowData(102, "kevin"));

        rowDataDataStreamSource.addSink(
                StarRocksSink.sink(
                        // the table structure
                        TableSchema.builder()
                                .field("score", DataTypes.INT())
                                .field("name", DataTypes.VARCHAR(255))
                                .build(),
                        // the sink options
                        StarRocksSinkOptions.builder()
                                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030?serverTimezone=Asia/Shanghai")
                                .withProperty("load-url", "127.0.0.1:8030")
                                .withProperty("username", "root")
                                .withProperty("password", "123456")
                                .withProperty("table-name", "flink_test")
                                .withProperty("database-name", "test")
                                .withProperty("sink.properties.column_separator", "\\x01")
                                .withProperty("sink.properties.row_delimiter", "\\x02")
                                .build(),
                        // set the slots with streamRowData
                        (slots, streamRowData) -> {
                            slots[0] = streamRowData.score;
                            slots[1] = streamRowData.name;
                        }
                )
        );
        System.out.println("Sink Success");
    }
}
