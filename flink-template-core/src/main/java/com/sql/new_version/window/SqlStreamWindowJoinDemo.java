package com.sql.new_version.window;

import com.model.UserIdClubTs;
import com.model.UserPosTs;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/joins/
 */
public class SqlStreamWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(600000);
        env.setParallelism(1);

        EnvironmentSettings newStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, newStreamSettings);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "86400000");

        // nc -l -p 8888
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("localhost", 8888);
        // nc -l -p 9999
        DataStreamSource<String> dataStreamSource2 = env.socketTextStream("localhost", 9999);


        DataStream<UserIdClubTs> leftWindowStream = dataStreamSource1.map(line -> {
            String[] split = line.split(",");
            String userId = split[0];
            String userClub = split[1];
            Long ts = Long.parseLong(split[2]);

            UserIdClubTs userIdClubTs = new UserIdClubTs();
            userIdClubTs.setUserId(userId);
            userIdClubTs.setUserClub(userClub);
            userIdClubTs.setTs(ts);

            return userIdClubTs;

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserIdClubTs>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(UserIdClubTs element) {
                return element.getTs();
            }
        });

        DataStream<UserPosTs> rightWindowStream = dataStreamSource2.map(line -> {
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

        Table leftTable = tableEnv.fromDataStream(leftWindowStream, "userId,userClub,ts,rt.rowtime");
        Table rightTable = tableEnv.fromDataStream(rightWindowStream, "userId,userPos,ts,rt.rowtime");

        tableEnv.createTemporaryView("left_table", leftTable);
        tableEnv.createTemporaryView("right_table", rightTable);

        String leftTableQuery = "select " +
                " userId,userClub,ts,tumble_rowtime(rt, interval '3' seconds)," +
                " tumble_start(rt, interval '3' second),tumble_end(rt, interval '3' second)" +
                " from left_table" +
                " group by tumble(rt,interval '3' second),userId,userClub,ts";

        String rightTableQuery = "select " +
                " userId,userPos,ts,tumble_rowtime(rt, interval '3' seconds)," +
                " tumble_start(rt, interval '3' second),tumble_end(rt, interval '3' second)" +
                " from right_table" +
                " group by tumble(rt,interval '3' second),userId,userPos,ts";

        // 对于不带窗口的全量连接，内部Join算子，会维护A、B两张表的哈希索引。
        // 当A表中插入一条数据时会查B的索引，寻找Join的条件数据，同时将本条数据加入A的索引。反之亦然。
        // 但是这种join会随着数据量的增加，哈希表维护成本可能会无限增长下去。
        // 解决方式是通过状态TTL等手段加以限制。
        String leftJoinTableQuery = "select " +
                " left_table.userId,userClub,userPos" +
                " from left_table inner join right_table" +
                " on left_table.userId=right_table.userId ";


        String leftJoinWindowTableQuery = "select t1.userId,userClub,userPos,w1_start from " +
                " ( select userId,userClub,ts,tumble_start(rt, interval '3' second) as w1_start" +
                " from left_table" +
                " group by tumble(rt,interval '3' second),userId,userClub,ts) t1" +
                " inner join" +
                " ( select " +
                " userId,userPos,ts,tumble_start(rt, interval '3' second) as w2_start" +
                " from right_table" +
                " group by tumble(rt,interval '3' second),userId,userPos,ts) t2" +
                " on t1.w1_start=t2.w2_start and t1.userId=t2.userId";

        Table leftResultTable = tableEnv.sqlQuery(leftTableQuery);
        Table rightResultTable = tableEnv.sqlQuery(rightTableQuery);
        Table joinResultTable = tableEnv.sqlQuery(leftJoinTableQuery);
        Table windowJoinResultTable = tableEnv.sqlQuery(leftJoinWindowTableQuery);


//        tableEnv.toAppendStream(leftResultTable, Row.class).print("LeftResult");
//        tableEnv.toAppendStream(rightResultTable, Row.class).print("RightResult");

//        tableEnv.toRetractStream(joinResultTable, Row.class).print("JoinResult");
        tableEnv.toRetractStream(windowJoinResultTable, Row.class).print("WindowJoinResult");

        env.execute();

    }
}

