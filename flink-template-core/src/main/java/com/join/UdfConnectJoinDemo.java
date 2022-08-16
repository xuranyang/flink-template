package com.join;

import com.join.model.UserJoinLeft;
import com.join.model.UserJoinRight;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

/**
 * Connect + keyBy + CoProcessFunction 实现自定义双流Join逻辑
 */
/*
一次性输入
nc -lp8888:
Elephant,maybe,1618842801000
Elephant,fy,1618842801001
LGD,ame,1618842801002
EHOME,chalice,1618842805000
OG,ana,26188428110002

一个一个依次输入
nc -lp9999:
maybe,2,1618842800999
chalice,3,1618842805000
xnova,5,1618842809000
ana,1,2618843809000

*/
public class UdfConnectJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /**
         * 老版本需要手动指定
         * 新版本默认就是EventTime
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<UserJoinLeft> dataStreamLeft = env.socketTextStream("localhost", 8888).map(line -> {
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

        DataStream<UserJoinRight> dataStreamRight = env.socketTextStream("localhost", 9999).map(line -> {
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

        OutputTag<String> leftJoinTag = new OutputTag<>("left-join", TypeInformation.of(String.class));
        OutputTag<String> rightJoinTag = new OutputTag<>("right-join", TypeInformation.of(String.class));

        /**
         * 自定义Process方法实现双流Join
         */
        SingleOutputStreamOperator<Tuple2<UserJoinLeft, UserJoinRight>> joinDataStream = dataStreamLeft
                .connect(dataStreamRight)
                .keyBy(UserJoinLeft::getUserId, UserJoinRight::getUserId)
                .process(new CoProcessFunction<UserJoinLeft, UserJoinRight, Tuple2<UserJoinLeft, UserJoinRight>>() {
                    private ValueState<UserJoinLeft> leftState;
                    private ValueState<UserJoinRight> rightState;
                    private ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化State状态
                        leftState = getRuntimeContext().getState(new ValueStateDescriptor<UserJoinLeft>("left-state", UserJoinLeft.class));
                        rightState = getRuntimeContext().getState(new ValueStateDescriptor<UserJoinRight>("right-state", UserJoinRight.class));
                        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", Long.class));
                    }

                    @Override
                    public void processElement1(UserJoinLeft userJoinLeft, CoProcessFunction<UserJoinLeft, UserJoinRight, Tuple2<UserJoinLeft, UserJoinRight>>.Context context, Collector<Tuple2<UserJoinLeft, UserJoinRight>> out) throws Exception {
                        if (rightState.value() == null) {
                            leftState.update(userJoinLeft);

                            //建立定时器，2秒后触发
                            Long ts = userJoinLeft.getTimestamp() + 2 * 1000L;
                            context.timerService().registerEventTimeTimer(ts);
                            timeState.update(ts);
                        } else {
                            // 直接输出到主流
                            out.collect(new Tuple2<>(userJoinLeft, rightState.value()));
                            // 删除定时器
                            context.timerService().deleteEventTimeTimer(timeState.value());
                            // 清空状态
                            rightState.clear();
                            timeState.clear();
                        }

                    }

                    @Override
                    public void processElement2(UserJoinRight userJoinRight, CoProcessFunction<UserJoinLeft, UserJoinRight, Tuple2<UserJoinLeft, UserJoinRight>>.Context context, Collector<Tuple2<UserJoinLeft, UserJoinRight>> out) throws Exception {

                        if (leftState.value() == null) {
                            rightState.update(userJoinRight);

                            //建立定时器，2秒后触发
                            Long ts = userJoinRight.getTimestamp() + 2 * 1000L;
                            context.timerService().registerEventTimeTimer(ts);
                            timeState.update(ts);
                        } else {
                            // 直接输出到主流
                            out.collect(new Tuple2<>(leftState.value(), userJoinRight));
                            // 删除定时器
                            context.timerService().deleteEventTimeTimer(timeState.value());
                            // 清空状态
                            leftState.clear();
                            timeState.clear();
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<UserJoinLeft, UserJoinRight, Tuple2<UserJoinLeft, UserJoinRight>>.OnTimerContext ctx, Collector<Tuple2<UserJoinLeft, UserJoinRight>> out) throws Exception {
                        if (leftState.value() != null) {
                            // 实现左连接
                            ctx.output(leftJoinTag, leftState.value().getUserId());
                        } else if (rightState.value() != null) {
                            // 实现右连接
                            ctx.output(rightJoinTag, rightState.value().getUserId());
                        }
                        leftState.clear();
                        rightState.clear();
                        timeState.clear();
                    }
                });

        DataStream<String> leftJoinResultDataStream = joinDataStream.getSideOutput(leftJoinTag);
        DataStream<String> rightJoinResultDataStream = joinDataStream.getSideOutput(rightJoinTag);

        joinDataStream.print("inner-join");
        leftJoinResultDataStream.print("left-join is null");
        rightJoinResultDataStream.print("right-join is null");

        env.execute();
    }
}
