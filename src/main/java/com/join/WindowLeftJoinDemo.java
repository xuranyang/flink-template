package com.join;

import com.join.model.UserJoinLeft;
import com.join.model.UserJoinRight;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 固定写法,够不灵活
 * stream.coGroup(otherStream)
 * .where(<KeySelector>)
 * .equalTo(<KeySelector>)
 * .window(<WindowAssigner>)
 * .apply(<CoGroupFunction>)
 */
public class WindowLeftJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /**
         * 老版本需要手动指定
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<UserJoinLeft> dataStreamLeft = env.readTextFile("src/main/java/com/data/join_data.txt").map(line -> {
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

        DataStream<UserJoinRight> dataStreamRight = env.readTextFile("src/main/java/com/data/join_data3.txt").map(line -> {
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
         * 双流left join
         */
        dataStreamLeft.coGroup(dataStreamRight)
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
                .apply(new CoGroupFunction<UserJoinLeft, UserJoinRight, String>() {
                    @Override
                    public void coGroup(Iterable<UserJoinLeft> userJoinLefts, Iterable<UserJoinRight> userJoinRights, Collector<String> out) throws Exception {
                        for (UserJoinLeft userJoinLeft : userJoinLefts) {
                            boolean isMatched = false;
                            String userId = userJoinLeft.getUserId();
                            String club = userJoinLeft.getClub();
                            for (UserJoinRight userJoinRight : userJoinRights) {
                                String position = userJoinRight.getPosition();
                                // 右流中有对应的记录
                                out.collect(club + "|" + userId + "|" + position);
                                isMatched = true;
                            }

                            if (!isMatched) {
                                // 右流中没有对应的记录
                                out.collect(club + "|" + userId + "|" + null);
                            }
                        }

                    }
                }).print();
        env.execute();
    }
}
