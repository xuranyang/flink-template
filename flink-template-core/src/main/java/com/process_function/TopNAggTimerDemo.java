package com.process_function;

import com.model.UserBehavior;
import com.model.UserItemWindowAgg;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;

//取一段时间内出现次数最多的TopN
public class TopNAggTimerDemo {
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

        dataStream.keyBy(UserBehavior::getItem)
                .timeWindow(Time.seconds(20), Time.seconds(10))
                // 只要是同一个窗口内的数据,每来一条数据,结果都会增量聚合一次
                .aggregate(new ItemIncrAgg(), new WindowItemIncrAggResult())
                .keyBy(UserItemWindowAgg::getWindowEnd)
                .process(new TopNItems(3))
                .print();

        env.execute();

    }

    public static class TopNItems extends KeyedProcessFunction<Long, UserItemWindowAgg, String> {
        private int topN;
        ListState<UserItemWindowAgg> itemViewCountListState;

        public TopNItems(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UserItemWindowAgg>("item-agg-list", UserItemWindowAgg.class));
//            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UserItemWindowAgg>("item-agg-list", TypeInformation.of(new TypeHint<UserItemWindowAgg>() {
//            })));
        }

        @Override
        public void processElement(UserItemWindowAgg userItemWindowAgg, Context context, Collector<String> collector) throws Exception {
            itemViewCountListState.add(userItemWindowAgg);
//            context.timerService().registerEventTimeTimer(userItemWindowAgg.getWindowEnd() + 1);
            context.timerService().registerEventTimeTimer(userItemWindowAgg.getWindowEnd());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<UserItemWindowAgg> userItemWindowAggArrayList = Lists.newArrayList(itemViewCountListState.get().iterator());

            userItemWindowAggArrayList.sort(new Comparator<UserItemWindowAgg>() {
                @Override
                public int compare(UserItemWindowAgg o1, UserItemWindowAgg o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

//            String res = String.format("Timer:%s,Window End Time:%s\n", timestamp, timestamp - 1);
            String res = String.format("Timer:%s,Window End Time:%s\n", timestamp, timestamp);
            for (int i = 0; i < Math.min(topN, userItemWindowAggArrayList.size()); i++) {
                UserItemWindowAgg userItemWindowAgg = userItemWindowAggArrayList.get(i);
                res += userItemWindowAgg + "\n";
            }

            out.collect(res);
        }
    }

    /**
     * createAccumulator：创建一个新的累加器，开始一个新的聚合。累加器是正在运行的聚合的状态。
     * add：将给定的输入添加到给定的累加器，并返回新的累加器值。
     * getResult：从累加器获取聚合结果。
     * merge：合并两个累加器，返回合并后的累加器的状态。只针对SessionWindow有效，对应滚动窗口、滑动窗口不会调用此方法
     */
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
