package com.process_function;

import com.model.UserBehavior;
import com.model.UserItemWindowAgg;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * 比TopNAggTime多了一个允许延迟allowedLateness之后,
 * 当延迟数据进来,会把这条延迟数据再add到ListState中,导致输出结果会比之前没有allowedLateness(不允许有迟到数据)时多1条,
 * 比如原来Top2为:BKB-3,MKB-2,
 * 新进来一条延迟数据BKB后,现在输出结果变为:BKB-4,BKB-3,MKB-2,出现【重复】
 * 如果直接先clear之前的ListState,再add到ListState的话,输出结果会变为：BKB-4,
 * 我们需要保留MKB的话,需要改用MapState,利用put去修改BKB的值,
 * 原来为:{BKB:3,MKB:2}
 * 新进来一条延迟数据BKB后,现在输出结果变为:{BKB:4,MKB:2},这样就符合我们的要求,
 * 同时要在 完成本次输出结果以后~下一个窗口到来前，这段时间内,把当前的MapState给clear掉,否则又会出现之前的【重复】情况,
 * 根据allowedLateness的允许迟到时间latency,添加一个windowEnd+latency的定时器,
 * 当该定时器触发时,说明所有迟到数据已经全部收集齐了,可以clear掉,后面还有迟到数据的话会进入侧输出流中,不会再进入该窗口影响输出
 *
 *
 * 注：当允许allowedLateness,有迟到数据进来注册一个已经过时的定时器(定时器的触发时间小于当前的WaterMark)，不会立即触发，会等到下一次WaterMark更新时触发
 */
public class TopNAggTimerAllowedLatenessDemo {
    private static int SECONDS = 2;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        根据数据源自动选择使用流还是批
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputDataStream = env.socketTextStream("localhost", 1234);
//        DataStream<String> inputDataStream = env.readTextFile("flink-template-core/src/main/java/com/data/timer_data.txt");

        DataStream<UserBehavior> dataStream = inputDataStream.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                // 处理掉 "-" 开头的脏数据
                if (!value.startsWith("-")) {
                    String[] fields = value.split(",");
                    out.collect(UserBehavior.builder()
                            .userId(fields[0])
                            .item(fields[1])
                            .timestamp(Long.valueOf(fields[2]))
                            .build());
                }
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp();
            }
        });

        OutputTag<UserBehavior> outputTag = new OutputTag<UserBehavior>("late-tag") {
        };

        SingleOutputStreamOperator<UserItemWindowAgg> windowAggStream = dataStream.keyBy(UserBehavior::getItem)
                .timeWindow(Time.seconds(20), Time.seconds(10))
                .allowedLateness(Time.seconds(SECONDS))
                .sideOutputLateData(outputTag)
                // 只要是同一个窗口内的数据,每来一条数据,结果都会增量聚合一次
                .aggregate(new ItemIncrAgg(), new WindowItemIncrAggResult());

        windowAggStream.print("agg");
        windowAggStream.getSideOutput(outputTag).print("late");

        windowAggStream.keyBy(UserItemWindowAgg::getWindowEnd)
                .process(new TopNItems(3))
                .print("res");

        env.execute();

    }

    public static class TopNItems extends KeyedProcessFunction<Long, UserItemWindowAgg, String> {
        private int topN;
        //        ListState<UserItemWindowAgg> itemViewCountListState;
        MapState<String, Long> itemViewCountMapState;


        public TopNItems(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
//            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UserItemWindowAgg>("item-agg-list", UserItemWindowAgg.class));
            itemViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("item-agg-map", String.class, Long.class));

        }

        @Override
        public void processElement(UserItemWindowAgg userItemWindowAgg, Context context, Collector<String> collector) throws Exception {
//            itemViewCountListState.add(userItemWindowAgg);
            itemViewCountMapState.put(userItemWindowAgg.getItem(), userItemWindowAgg.getCount());

//            context.timerService().registerEventTimeTimer(userItemWindowAgg.getWindowEnd() + 1);
            context.timerService().registerEventTimeTimer(userItemWindowAgg.getWindowEnd());

            // 注册一个1分钟之后的定时器，用来清空状态
            context.timerService().registerEventTimeTimer(userItemWindowAgg.getWindowEnd() + SECONDS * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = ctx.getCurrentKey();
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == windowEnd + SECONDS * 1000L) {
                System.out.println("[Clear MapState]");
                itemViewCountMapState.clear();
//                return;
            }


            // 定时器触发，当前已收集到所有数据，排序输出
//            ArrayList<UserItemWindowAgg> userItemWindowAggArrayList = Lists.newArrayList(itemViewCountListState.get().iterator());
            ArrayList<Map.Entry<String, Long>> userItemWindowAggArrayList = Lists.newArrayList(itemViewCountMapState.entries());


            userItemWindowAggArrayList.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

//            String res = String.format("Timer:%s,Window End Time:%s\n", timestamp, timestamp - 1);
            String res = String.format("Timer:%s,Window End Time:%s\n", timestamp, timestamp);
            for (int i = 0; i < Math.min(topN, userItemWindowAggArrayList.size()); i++) {
//                UserItemWindowAgg userItemWindowAgg = userItemWindowAggArrayList.get(i);
                Map.Entry<String, Long> userItemWindowAgg = userItemWindowAggArrayList.get(i);

                res += userItemWindowAgg + "\n";
            }

            out.collect(res);
        }
    }

    // 增量聚合
    public static class ItemIncrAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            System.out.println(value + ":" + (accumulator + 1));
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class WindowItemIncrAggResult implements WindowFunction<Long, UserItemWindowAgg, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<UserItemWindowAgg> out) throws Exception {
            String item = key;
            Long windowEnd = window.getEnd();
            Long aggCount = input.iterator().next();
            out.collect(new UserItemWindowAgg(item, windowEnd, aggCount));
        }
    }
}
