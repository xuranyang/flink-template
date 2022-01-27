package com.join;

import com.join.model.UserJoinLeft;
import com.join.model.UserJoinRight;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * stream.keyBy()
 * .intervalJoin(otherStream.keyBy())
 * .between(lowerBound,upperBound)
 * .process(<ProcessJoinFunction>)
 *
 * 具体实现逻辑可以参考
 * https://www.jianshu.com/p/45ec888332df
 */
public class WindowIntervalJoinDemo {
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

        DataStream<UserJoinRight> dataStreamRight = env.readTextFile("flink-template-core/src/main/java/com/data/join_data4.txt").map(line -> {
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
         * 双流interval join
         */
        dataStreamLeft.keyBy(UserJoinLeft::getUserId)
                .intervalJoin(dataStreamRight.keyBy(UserJoinRight::getUserId))
                .between(Time.seconds(-1), Time.seconds(1))
                .process(new ProcessJoinFunction<UserJoinLeft, UserJoinRight, String>() {
                    @Override
                    public void processElement(UserJoinLeft userJoinLeft, UserJoinRight userJoinRight, Context ctx, Collector<String> out) throws Exception {
                        String userId = userJoinLeft.getUserId();
                        String club = userJoinLeft.getClub();
                        String position = userJoinRight.getPosition();
                        out.collect(club + "|" + userId + "|" + position);
                    }
                }).print();

        env.execute();
    }
}
