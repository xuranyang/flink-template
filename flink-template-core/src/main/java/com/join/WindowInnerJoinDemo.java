package com.join;

import com.join.model.UserJoinLeft;
import com.join.model.UserJoinRight;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 固定写法,够不灵活
 * stream.join(otherStream)
 * .where(<KeySelector>)
 * .equalTo(<KeySelector>)
 * .window(<WindowAssigner>)
 * .apply(<JoinFunction>)
 */
public class WindowInnerJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /**
         * 老版本需要手动指定
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<UserJoinLeft> dataStreamLeft = env.readTextFile("flink-template-core/src/main/java/com/data/join_data.txt").map(line -> {
            String[] fields = line.split(",");
            String club = fields[0];
            String userId = fields[1];
            Long timestamp = Long.valueOf(fields[2]);
            return new UserJoinLeft(club, userId, timestamp);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserJoinLeft>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserJoinLeft userJoinLeft) {
                return userJoinLeft.getTimestamp();
            }
        });

        String filePath="flink-template-core/src/main/java/com/data/join_data2.txt";
//        String filePath="flink-template-core/src/main/java/com/data/join_data4.txt";

        DataStream<UserJoinRight> dataStreamRight = env.readTextFile(filePath).map(line -> {
            String[] fields = line.split(",");
            String userId = fields[0];
            String position = fields[1];

            Long timestamp = Long.valueOf(fields[2]);
            return new UserJoinRight(userId, position, timestamp);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserJoinRight>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserJoinRight userJoinRight) {
                return userJoinRight.getTimestamp();
            }
        });

        /**
         * 双流inner join
         */
        dataStreamLeft.join(dataStreamRight)
                .where(new KeySelector<UserJoinLeft, String>() {
                    @Override
                    public String getKey(UserJoinLeft userJoinLeft) throws Exception {
                        return userJoinLeft.getUserId();
                    }
                })
                .equalTo(new KeySelector<UserJoinRight, String>() {
                    @Override
                    public String getKey(UserJoinRight userJoinRight) throws Exception {
                        return userJoinRight.getUserId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<UserJoinLeft, UserJoinRight, String>() {
                    @Override
                    public String join(UserJoinLeft userJoinLeft, UserJoinRight userJoinRight) throws Exception {
                        String userId = userJoinLeft.getUserId();
                        String club = userJoinLeft.getClub();
                        String position = userJoinRight.getPosition();
                        return club + "|" + userId + "|" + position;
                    }
                }).print();

        env.execute();
    }
}
