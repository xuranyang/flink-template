package com.sql.new_version;

import com.model.UserPosTs;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlKafkaJoinMysqlTableTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);

        DataStream<UserPosTs> lefDataStream = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            String userId = split[0];
            String userPos = split[1];
            Long ts = Long.parseLong(split[2]);

            UserPosTs userPosTs = new UserPosTs();
            userPosTs.setUserId(userId);
            userPosTs.setUserPos(userPos);
            userPosTs.setTs(ts);

            return userPosTs;

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserPosTs>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(UserPosTs element) {
                return element.getTs();
            }
        });
        Table leftTable = tableEnv.fromDataStream(lefDataStream, "userId,userPos,ts,rt.rowtime");
        tableEnv.createTemporaryView("left_table", leftTable);

        // executeSql or sqlUpdate
        tableEnv.executeSql("CREATE TABLE flink_test_table(" +
                "name STRING," +
                "club STRING" +
                ")" +
                "WITH" +
                "(" +
                "   'connector.type' = 'jdbc'," +
                "   'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink_test_db?serverTimezone=UTC'," +
                "   'connector.table' = 'mysql_test_table'," +
                "   'connector.driver' = 'com.mysql.cj.jdbc.Driver'," +
                "   'connector.username' = 'root'," +
                "   'connector.password' = '123456'," +
                "   'connector.lookup.cache.max-rows' = '5000'," +
                "   'connector.lookup.cache.ttl' = '10min'" +
                ")");

        String querySql = "SELECT userId,userPos,club" +
                " FROM left_table inner join flink_test_table" +
                " on left_table.userId=flink_test_table.name";
        Table table = tableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("[KafkaJoinMysqlInfo]");

    }
}
